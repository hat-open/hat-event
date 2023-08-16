import asyncio
import contextlib
import functools
import itertools
import logging
import typing

from hat import aio
from hat import chatter
from hat import util

from hat.event.syncer import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class ClientInfo(typing.NamedTuple):
    """Client connection information"""
    name: str
    synced: bool


StateCb: typing.TypeAlias = typing.Callable[[list[ClientInfo]], None]
"""Syncer state change callback"""

QueryCb: typing.TypeAlias = typing.Callable[[common.EventId],
                                            typing.AsyncIterable[list[common.Event]]]  # NOQA
"""Query callback"""


async def listen(address: str,
                 query_cb: QueryCb | None = None,
                 subscriptions: list[common.EventType] = [('*',)],
                 token: str | None = None
                 ) -> 'Server':
    """Create listening syncer server"""
    server = Server()
    server._query_cb = query_cb
    server._subscription = common.Subscription(subscriptions)
    server._token = token
    server._state = {}
    server._next_client_ids = itertools.count(1)
    server._state_cbs = util.CallbackRegistry()
    server._notify_cbs = util.CallbackRegistry()
    server._clients = {}

    server._server = await chatter.listen(sbs_repo=common.sbs_repo,
                                          address=address,
                                          connection_cb=server._on_connection,
                                          bind_connections=False)

    mlog.debug("listening on %s", address)
    return server


class Server(aio.Resource):
    """Syncer server"""

    @property
    def async_group(self):
        """Async group"""
        return self._server.async_group

    @property
    def state(self) -> list[ClientInfo]:
        """State of all active connections"""
        return list(self._state.values())

    def register_state_cb(self,
                          cb: StateCb
                          ) -> util.RegisterCallbackHandle:
        """Register state change callback"""
        return self._state_cbs.register(cb)

    def notify(self, events: list[common.Event]):
        """Notify clients of new events"""
        self._notify_cbs.notify(events)

    async def flush(self):
        """Send flush requests and wait for flush responses"""
        if not self.is_open:
            await self.wait_closed()
            return

        if not self._clients:
            return

        await asyncio.wait([self.async_group.spawn(client.flush)
                            for client in self._clients.values()])

    def _on_connection(self, conn):
        self.async_group.spawn(self._connection_loop, conn)

    def _update_client_info(self, client_id, client_info):
        self._state[client_id] = client_info
        self._state_cbs.notify(list(self._state.values()))

    def _remove_client_info(self, client_id):
        if self._state.pop(client_id, None):
            self._state_cbs.notify(list(self._state.values()))

    async def _connection_loop(self, conn):
        mlog.debug("starting new connection loop")

        client_id = None
        try:
            mlog.debug("waiting for incomming message")
            req_msg = await conn.receive()
            req_msg_type = req_msg.data.module, req_msg.data.type

            if req_msg_type != ('HatSyncer', 'MsgInitReq'):
                raise Exception('unsupported message type')

            mlog.debug("received init request")
            req = common.syncer_init_req_from_sbs(req_msg.data.data)

            if self._token is not None and req.client_token != self._token:
                res = 'invalid client token'

            else:
                res = None

            mlog.debug("sending init response")
            res_msg_data = chatter.Data(
                module='HatSyncer',
                type='MsgInitRes',
                data=common.syncer_init_res_to_sbs(res))
            conn.send(res_msg_data, conv=req_msg.conv)

            if res is not None:
                await conn.drain()
                raise Exception(res)

            client_id = next(self._next_client_ids)
            last_event_id = req.last_event_id
            subscription = self._subscription.intersection(
                common.Subscription(req.subscriptions))
            client_info = ClientInfo(name=req.client_name,
                                     synced=False)

            self._update_client_info(client_id, client_info)

            mlog.debug("creating client")
            synced_cb = functools.partial(self._update_client_info, client_id,
                                          client_info._replace(synced=True))
            client = _Client(query_cb=self._query_cb,
                             notify_cbs=self._notify_cbs,
                             conn=conn,
                             last_event_id=last_event_id,
                             subscription=subscription,
                             synced_cb=synced_cb)

            self._clients[client_id] = client
            try:
                await client.wait_closing()

            finally:
                self._clients.pop(client_id)
                await aio.uncancellable(client.async_close())

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("connection loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("closing client connection loop")
            conn.close()
            self._remove_client_info(client_id)
            await aio.uncancellable(conn.async_close())


class _Client(aio.Resource):

    def __init__(self, query_cb, notify_cbs, conn, last_event_id, subscription,
                 synced_cb):
        self._query_cb = query_cb
        self._notify_cbs = notify_cbs
        self._conn = conn
        self._last_event_id = last_event_id
        self._async_group = aio.Group()

        self._receiver = _Receiver(conn)
        self.async_group.spawn(aio.call_on_done, self._receiver.wait_closing(),
                               self.close)

        self._sender = _Sender(conn, last_event_id, subscription, synced_cb,
                               self._receiver)
        self.async_group.spawn(aio.call_on_done, self._sender.wait_closing(),
                               self.close)

        self.async_group.spawn(self._client_loop)
        self.async_group.spawn(aio.call_on_done, self._conn.wait_closing(),
                               self.close)

    @property
    def async_group(self):
        return self._async_group

    async def flush(self):
        try:
            await self._sender.send_flush()

        except Exception:
            await self._sender.wait_closed()

    async def _client_loop(self):
        mlog.debug("starting new client loop")
        events_queue = aio.Queue()
        is_synced = False

        async def cleanup():
            self.close()

            if is_synced:
                with contextlib.suppress(ConnectionError):
                    while not events_queue.empty():
                        self._sender.send_events(events_queue.get_nowait())

            await self._sender.async_close()
            await self._receiver.async_close()

        try:
            with self._notify_cbs.register(events_queue.put_nowait):
                mlog.debug("query backend")
                if self._query_cb:
                    async for events in self._query_cb(self._last_event_id):
                        self._sender.send_events(events)

                mlog.debug("sending synced")
                self._sender.send_synced()
                is_synced = True

                while True:
                    events = await events_queue.get()
                    self._sender.send_events(events)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("client loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping client loop")
            await aio.uncancellable(cleanup())


class _Sender(aio.Resource):

    def __init__(self, conn, last_event_id, subscription, synced_cb, receiver):
        self._conn = conn
        self._last_event_id = last_event_id
        self._subscription = subscription
        self._synced_cb = synced_cb
        self._receiver = receiver
        self._send_queue = aio.Queue()
        self._async_group = aio.Group()

        self.async_group.spawn(self._send_loop)

    @property
    def async_group(self):
        return self._async_group

    def send_events(self, events):
        try:
            self._send_queue.put_nowait(('events', events))

        except aio.QueueClosedError:
            raise ConnectionError()

    def send_synced(self):
        try:
            self._send_queue.put_nowait(('synced', None))

        except aio.QueueClosedError:
            raise ConnectionError()

    async def send_flush(self):
        try:
            future = asyncio.Future()
            self._send_queue.put_nowait(('flush', future))
            await future

        except aio.QueueClosedError:
            raise ConnectionError()

    async def _send_loop(self):
        is_synced = False

        async def cleanup():
            self.close()
            self._send_queue.close()

            while not self._send_queue.empty():
                msg_type, msg_data = self._send_queue.get_nowait()

                with contextlib.suppress(Exception):
                    if msg_type == 'events' and is_synced:
                        self._send_events(msg_data)

                    elif msg_type == 'flush':
                        self._send_flush(msg_data)

            if is_synced:
                with contextlib.suppress(Exception):
                    future = asyncio.Future()
                    self._send_flush(future)
                    await future

            with contextlib.suppress(Exception):
                await self._conn.drain()

        try:
            while True:
                msg_type, msg_data = await self._send_queue.get()

                if msg_type == 'events':
                    self._send_events(msg_data)

                elif msg_type == 'synced':
                    self._send_synced()
                    is_synced = True
                    self._synced_cb()

                elif msg_type == 'flush':
                    self._send_flush(msg_data)

                else:
                    raise ValueError('unsupported message')

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("send loop error: %s", e, exc_info=e)

        finally:
            await aio.uncancellable(cleanup())

    def _send_events(self, events):
        if not events:
            return

        if events[0].event_id.server != self._last_event_id.server:
            return

        if events[0].event_id.session < self._last_event_id.session:
            return

        if events[0].event_id.session == self._last_event_id.session:
            events = (event for event in events
                      if event.event_id > self._last_event_id)

        events = [event for event in events
                  if self._subscription.matches(event.event_type)]
        if not events:
            return

        mlog.debug("sending events")
        data = chatter.Data(module='HatSyncer',
                            type='MsgEvents',
                            data=[common.event_to_sbs(e)
                                  for e in events])
        self._conn.send(data)
        self._last_event_id = events[-1].event_id

    def _send_synced(self):
        self._conn.send(chatter.Data(module='HatSyncer',
                                     type='MsgSynced',
                                     data=None))

    def _send_flush(self, future):
        try:
            conv = self._conn.send(chatter.Data(module='HatSyncer',
                                                type='MsgFlushReq',
                                                data=None),
                                   last=False)
            self._receiver.add_flush_future(conv, future)

        except Exception:
            future.set_result(None)
            raise


class _Receiver(aio.Resource):

    def __init__(self, conn):
        self._conn = conn
        self._flush_futures = {}
        self._async_group = aio.Group()

        self.async_group.spawn(self._receive_loop)

    @property
    def async_group(self):
        return self._async_group

    def add_flush_future(self, conv, future):
        if not self.is_open:
            raise ConnectionError()

        self._flush_futures[conv] = future

    async def _receive_loop(self):
        try:
            while True:
                msg = await self._conn.receive()
                msg_type = msg.data.module, msg.data.type

                if msg_type != ('HatSyncer', 'MsgFlushRes'):
                    raise Exception("unsupported message type")

                flush_future = self._flush_futures.pop(msg.conv, None)
                if flush_future and not flush_future.done():
                    flush_future.set_result(None)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("receive loop error: %s", e, exc_info=e)

        finally:
            self.close()

            for flush_future in self._flush_futures.values():
                if not flush_future.done():
                    flush_future.set_result(None)
