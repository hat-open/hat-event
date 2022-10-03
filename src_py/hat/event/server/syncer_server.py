"""Syncer server"""

import asyncio
import collections
import contextlib
import functools
import itertools
import logging
import typing

from hat import aio
from hat import chatter
from hat import json
from hat import util
from hat.event.server import common
import hat.event.common.data


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class ClientInfo(typing.NamedTuple):
    """Client connection information"""
    name: str
    synced: bool


StateCb = typing.Callable[[typing.List[ClientInfo]], None]
"""Syncer state change callback"""


async def create_syncer_server(conf: json.Data,
                               backend: common.Backend
                               ) -> 'SyncerServer':
    """Create syncer server

    Args:
        conf: configuration defined by
            ``hat-event://main.yaml#/definitions/syncer_server``
        backend: backend

    """
    srv = SyncerServer()
    srv._backend = backend
    srv._state = {}
    srv._next_client_ids = itertools.count(1)
    srv._state_cbs = util.CallbackRegistry()
    srv._clients = {}

    srv._server = await chatter.listen(sbs_repo=common.sbs_repo,
                                       address=conf['address'],
                                       connection_cb=srv._on_connection,
                                       bind_connections=False)
    mlog.debug("listening on %s", conf['address'])
    return srv


class SyncerServer(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._server.async_group

    @property
    def state(self) -> typing.Iterable[ClientInfo]:
        """State of all active connections"""
        return self._state.values()

    def register_state_cb(self,
                          cb: StateCb
                          ) -> util.RegisterCallbackHandle:
        """Register state change callback"""
        return self._state_cbs.register(cb)

    async def flush(self):
        if not self.is_open:
            await self.wait_closed()
            return

        await asyncio.wait([self.async_group.spawn(client.flush)
                            for client in self._clients.values()])

    def _on_connection(self, conn):
        self.async_group.spawn(self._connection_loop, conn)

    def _update_client_info(self, client_id, name, synced):
        self._state[client_id] = ClientInfo(name, synced)
        self._state_cbs.notify(list(self._state.values()))

    def _remove_client_info(self, client_id):
        if self._state.pop(client_id, None):
            self._state_cbs.notify(list(self._state.values()))

    async def _connection_loop(self, conn):
        mlog.debug("starting new connection loop")

        client_id = None
        try:
            mlog.debug("waiting for incomming message")
            msg = await conn.receive()
            msg_type = msg.data.module, msg.data.type

            if msg_type != ('HatSyncer', 'MsgReq'):
                raise Exception('unsupported message type')

            mlog.debug("received request")
            conn.async_group.spawn(_receive_loop, conn)

            msg_req = hat.event.common.data.syncer_req_from_sbs(msg.data.data)
            client_id = next(self._next_client_ids)
            name = msg_req.client_name
            last_event_id = msg_req.last_event_id
            self._update_client_info(client_id, name, False)

            mlog.debug("creating client")
            synced_cb = functools.partial(self._update_client_info, client_id,
                                          name, True)
            client = _Client(backend=self._backend,
                             conn=conn,
                             client_id=client_id,
                             name=name,
                             last_event_id=last_event_id,
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

    def __init__(self, backend, conn, client_id, name, last_event_id,
                 synced_cb):
        self._backend = backend
        self._conn = conn
        self._client_id = client_id
        self._name = name
        self._last_event_id = last_event_id
        self._synced_cb = synced_cb
        self._synced = False
        self._events_queue = aio.Queue()
        self._async_group = aio.Group()

        self.async_group.spawn(self._client_loop)
        self.async_group.spawn(aio.call_on_done, self._conn.wait_closing(),
                               self.close)

    @property
    def async_group(self):
        return self._async_group

    async def flush(self):
        with contextlib.suppress(aio.QueueClosedError):
            future = asyncio.Future()
            self._events_queue.put_nowait(([], future))
            await future

    async def _client_loop(self):
        mlog.debug("starting new client loop")
        future = None

        async def cleanup():
            nonlocal future

            self.close()
            self._events_queue.close()

            futures = collections.deque()
            if future:
                futures.append(future)

            send = self._synced
            while not self._events_queue.empty():
                events, future = self._events_queue.get_nowait()
                if future:
                    futures.append(future)
                if not send:
                    continue

                try:
                    self._send_events(events)

                except Exception:
                    send = False

            with contextlib.suppress(Exception):
                await self._conn.drain()

            for future in futures:
                if not future.done():
                    future.set_result(None)

        def on_events(events):
            self._events_queue.put_nowait((events, None))

        try:
            with self._backend.register_flushed_events_cb(on_events):
                mlog.debug("query backend")
                async for events in self._backend.query_flushed(
                        self._last_event_id):
                    self._send_events(events)

                mlog.debug("sending synced")
                self._conn.send(chatter.Data(module='HatSyncer',
                                             type='MsgSynced',
                                             data=None))
                self._set_synced()

                while True:
                    events, future = await self._events_queue.get()
                    self._send_events(events)
                    if not future or future.done():
                        continue

                    await self._conn.drain()
                    if not future.done():
                        future.set_result(None)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("client loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping client loop")
            await aio.uncancellable(cleanup())

    def _set_synced(self):
        self._synced = True
        self._synced_cb()

    def _send_events(self, events):
        if not events:
            return

        if events[0].event_id.server != self._last_event_id.server:
            return

        if events[0].event_id.session < self._last_event_id.session:
            return

        if events[0].event_id.session == self._last_event_id.session:
            events = [event for event in events
                      if event.event_id > self._last_event_id]
            if not events:
                return

        mlog.debug("sending events")
        data = chatter.Data(module='HatSyncer',
                            type='MsgEvents',
                            data=[common.event_to_sbs(e)
                                  for e in events])
        self._conn.send(data)
        self._last_event_id = events[-1].event_id


async def _receive_loop(conn):
    try:
        await conn.receive()
        raise Exception("unexpected request")

    except ConnectionError:
        pass

    except Exception as e:
        mlog.error("receive loop error: %s", e, exc_info=e)

    finally:
        conn.close()
