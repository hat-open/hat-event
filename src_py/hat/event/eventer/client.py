import asyncio
import contextlib
import logging
import typing

from hat import aio
from hat import chatter
from hat import util
import hat.monitor.client

from hat.event import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

RunClientCb = typing.Callable[['Client'], typing.Awaitable]
"""Event client run callback coroutine"""


async def connect(address: str,
                  subscriptions: typing.List[common.EventType] = [],
                  **kwargs
                  ) -> 'Client':
    """Connect to eventer server

    For address format see `hat.chatter.connect` coroutine.

    According to Event Server specification, each subscription is event
    type identifier which can contain special subtypes ``?`` and ``*``.
    Subtype ``?`` can occure at any position inside event type identifier
    and is used as replacement for any single subtype. Subtype ``*`` is valid
    only as last subtype in event type identifier and is used as replacement
    for zero or more arbitrary subtypes.

    If subscription is empty list, client doesn't subscribe for any events and
    will not receive server's notifications.

    Args:
        address: event server's address
        subscriptions: subscriptions
        kwargs: additional arguments passed to `hat.chatter.connect` coroutine

    """
    client = Client()
    client._conv_futures = {}
    client._event_queue = aio.Queue()

    client._conn = await chatter.connect(common.sbs_repo, address, **kwargs)

    if subscriptions:
        client._conn.send(chatter.Data(module='HatEventer',
                                       type='MsgSubscribe',
                                       data=[list(i) for i in subscriptions]))

    client.async_group.spawn(client._receive_loop)
    return client


class Client(aio.Resource):
    """Eventer client

    For creating new client see `connect` coroutine.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._conn.async_group

    async def receive(self) -> typing.List[common.Event]:
        """Receive subscribed event notifications

        Raises:
            ConnectionError

        """
        try:
            return await self._event_queue.get()

        except aio.QueueClosedError:
            raise ConnectionError()

    def register(self, events: typing.List[common.RegisterEvent]):
        """Register events

        Raises:
            ConnectionError

        """
        msg_data = chatter.Data(module='HatEventer',
                                type='MsgRegisterReq',
                                data=[common.register_event_to_sbs(i)
                                      for i in events])
        self._conn.send(msg_data)

    async def register_with_response(self,
                                     events: typing.List[common.RegisterEvent]
                                     ) -> typing.List[typing.Optional[common.Event]]:  # NOQA
        """Register events

        Each `common.RegisterEvent` from `events` is paired with results
        `common.Event` if new event was successfuly created or ``None`` is new
        event could not be created.

        Raises:
            ConnectionError

        """
        msg_data = chatter.Data(module='HatEventer',
                                type='MsgRegisterReq',
                                data=[common.register_event_to_sbs(i)
                                      for i in events])
        conv = self._conn.send(msg_data, last=False)
        return await self._wait_conv_res(conv)

    async def query(self,
                    data: common.QueryData
                    ) -> typing.List[common.Event]:
        """Query events from server

        Raises:
            ConnectionError

        """
        msg_data = chatter.Data(module='HatEventer',
                                type='MsgQueryReq',
                                data=common.query_to_sbs(data))
        conv = self._conn.send(msg_data, last=False)
        return await self._wait_conv_res(conv)

    async def _receive_loop(self):
        mlog.debug("starting receive loop")
        try:
            while True:
                mlog.debug("waiting for incoming message")
                msg = await self._conn.receive()
                msg_type = msg.data.module, msg.data.type

                if msg_type == ('HatEventer', 'MsgNotify'):
                    mlog.debug("received event notification")
                    self._process_msg_notify(msg)

                elif msg_type == ('HatEventer', 'MsgQueryRes'):
                    mlog.debug("received query response")
                    self._process_msg_query_res(msg)

                elif msg_type == ('HatEventer', 'MsgRegisterRes'):
                    mlog.debug("received register response")
                    self._process_msg_register_res(msg)

                else:
                    raise Exception("unsupported message type")

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("read loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping receive loop")
            self.close()
            self._event_queue.close()
            for f in self._conv_futures.values():
                if not f.done():
                    f.set_exception(ConnectionError())

    async def _wait_conv_res(self, conv):
        if not self.is_open:
            raise ConnectionError()

        response_future = asyncio.Future()
        self._conv_futures[conv] = response_future
        try:
            return await response_future
        finally:
            self._conv_futures.pop(conv, None)

    def _process_msg_notify(self, msg):
        events = [common.event_from_sbs(e) for e in msg.data.data]
        self._event_queue.put_nowait(events)

    def _process_msg_query_res(self, msg):
        f = self._conv_futures.get(msg.conv)
        if not f or f.done():
            return
        events = [common.event_from_sbs(e) for e in msg.data.data]
        f.set_result(events)

    def _process_msg_register_res(self, msg):
        f = self._conv_futures.get(msg.conv)
        if not f or f.done():
            return
        events = [common.event_from_sbs(e) if t == 'event' else None
                  for t, e in msg.data.data]
        f.set_result(events)


async def run_client(monitor_client: hat.monitor.client.Client,
                     server_group: str,
                     run_cb: RunClientCb,
                     subscriptions: typing.List[common.EventType] = [],
                     reconnect_delay: float = 0.5
                     ) -> typing.Any:
    """Continuously communicate with currently active Event Server

    This function tries to establish active connection with Event Server
    within monitor component group `server_group`. Once this connection is
    established, `run_cb` is called with currently active `Client`
    instance. Once connection to Event Server is closed or new active Event
    Server is detected, execution of `run_cb` is canceled. If new
    connection to Event Server is successfully established,
    `run_cb` is called with new instance of `Client`.

    `run_cb` is called when:
        * new active `Client` is created

    `run_cb` execution is cancelled when:
        * `run_client` finishes execution
        * connection to Event Server is closed
        * different active Event Server is detected from Monitor Server's list
          of components

    `run_client` finishes execution when:
        * connection to Monitor Server is closed
        * `run_cb` finishes execution (by returning value or raising
          exception, other than `asyncio.CancelledError`)

    Return value of this function is the same as return value of
    `run_cb`. If `run_cb` finishes by raising exception or if
    connection to Monitor Server is closed, ConnectionError is raised.

    If execution of `run_client` is canceled while `run_cb` is
    running, connection to Event Server is closed after `run_cb`
    cancellation finishes.

    `reconnect_delay` defines delay in seconds before trying to reconnect to
    Event Server.

    """
    async_group = aio.Group()
    address_queue = aio.Queue()
    async_group.spawn(aio.call_on_done, monitor_client.wait_closing(),
                      address_queue.close)
    async_group.spawn(_address_loop, monitor_client, server_group,
                      address_queue)

    address = None
    try:
        while True:
            while not address:
                address = await address_queue.get_until_empty()

            async with async_group.create_subgroup() as subgroup:
                address_future = subgroup.spawn(address_queue.get_until_empty)
                client_future = subgroup.spawn(_client_loop, address,
                                               subscriptions, reconnect_delay,
                                               run_cb)

                await asyncio.wait([address_future, client_future],
                                   return_when=asyncio.FIRST_COMPLETED)

                if address_future.done():
                    address = address_future.result()
                else:
                    return client_future.result()

    except aio.QueueClosedError:
        raise ConnectionError()

    finally:
        await aio.uncancellable(async_group.async_close())


async def _address_loop(monitor_client, server_group, address_queue):
    last_address = None
    changes = aio.Queue()

    with monitor_client.register_change_cb(lambda: changes.put_nowait(None)):
        while True:
            info = util.first(monitor_client.components, lambda c: (
                c.group == server_group and
                c.blessing_req.token is not None and
                c.blessing_req.token == c.blessing_res.token))
            address = info.data['eventer_server_address'] if info else None

            if (address and address != last_address and
                    not address_queue.is_closed):
                mlog.debug("new server address: %s", address)
                last_address = address
                address_queue.put_nowait(address)

            await changes.get()


async def _client_loop(address, subscriptions, reconnect_delay, run_cb):
    while True:
        client = None
        try:
            mlog.debug("connecting to server %s", address)
            try:
                client = await connect(address, subscriptions)
            except Exception as e:
                mlog.warning("error connecting to server: %s", e, exc_info=e)
                await asyncio.sleep(reconnect_delay)
                continue

            mlog.debug("connected to server - running run_cb")

            subgroup = client.async_group.create_subgroup()
            try:
                run_future = subgroup.spawn(run_cb, client)
                await asyncio.wait([run_future])

                with contextlib.suppress(asyncio.CancelledError):
                    return run_future.result()

            finally:
                await aio.uncancellable(subgroup.async_close())

        finally:
            if client:
                await aio.uncancellable(client.async_close())

        mlog.debug("connection to server closed")
        await asyncio.sleep(reconnect_delay)