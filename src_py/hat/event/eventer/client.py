import asyncio
import logging
import typing

from hat import aio
from hat import chatter
from hat import util
import hat.monitor.client

from hat.event import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


async def connect(address: str,
                  subscriptions: list[common.EventType] = [],
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

    async def receive(self) -> list[common.Event]:
        """Receive subscribed event notifications

        Raises:
            ConnectionError

        """
        try:
            return await self._event_queue.get()

        except aio.QueueClosedError:
            raise ConnectionError()

    def register(self, events: list[common.RegisterEvent]):
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
                                     events: list[common.RegisterEvent]
                                     ) -> list[common.Event | None]:
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
                    ) -> list[common.Event]:
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


Runner: typing.TypeAlias = aio.Resource
"""Component runner"""

ComponentCb: typing.TypeAlias = typing.Callable[[Client], Runner]
"""Component callback"""


class Component(aio.Resource):
    """Eventer component

    High-level interface for communication with Event Server, based on
    information obtained from Monitor Server.

    Instance of this class tries to establish active connection with
    Event Server within monitor component group `server_group`. Once this
    connection is established, `component_cb` is called with currently active
    `Client` instance. Result of calling `component_cb` should be `Runner`
    representing user defined components activity associated with connection
    to active Event Server. Once connection to Event Server is closed or new
    active Event Server is detected, associated `Runner` is closed. If new
    connection to Event Server is successfully established,
    `component_cb` will be called again to create new `Runner` associated with
    new instance of `Client`.

    `component_cb` is called when:
        * new active `Client` is created

    `Runner`, returned by `component_cb`, is closed when:
        * `Component` is closed
        * connection to Event Server is closed
        * different active Event Server is detected from Monitor Server's list
          of components

    `Component` is closed when:
        * connection to Monitor Server is closed
        * `Runner`, returned by `component_cb`, is closed by causes other
          than change of active Event Server

    `reconnect_delay` defines delay in seconds before trying to reconnect to
    Event Server.

    """

    def __init__(self,
                 monitor_client: hat.monitor.client.Client,
                 server_group: str,
                 component_cb: ComponentCb,
                 subscriptions: list[common.EventType] = [],
                 reconnect_delay: float = 0.5):
        self._monitor_client = monitor_client
        self._server_group = server_group
        self._component_cb = component_cb
        self._subscriptions = subscriptions
        self._reconnect_delay = reconnect_delay
        self._async_group = aio.Group()
        self._address_queue = aio.Queue()

        self.async_group.spawn(self._monitor_loop)
        self.async_group.spawn(self._address_loop)

        self.async_group.spawn(aio.call_on_done, monitor_client.wait_closing(),
                               self.close)

    @property
    def async_group(self):
        return self._async_group

    async def _monitor_loop(self):
        last_address = None
        changes = aio.Queue()

        def on_change():
            changes.put_nowait(None)

        def info_filter(info):
            return (info.group == self._server_group and
                    info.blessing_req.token is not None and
                    info.blessing_req.token == info.blessing_res.token)

        try:
            with self._monitor_client.register_change_cb(on_change):
                while True:
                    info = util.first(self._monitor_client.components,
                                      info_filter)
                    address = (info.data.get('eventer_server_address')
                               if info else None)

                    if address and address != last_address:
                        mlog.debug("new server address: %s", address)
                        last_address = address
                        self._address_queue.put_nowait(address)

                    await changes.get()

        except Exception as e:
            mlog.error("component monitor loop error: %s", e, exc_info=e)

        finally:
            self.close()

    async def _address_loop(self):
        try:
            address = None
            while True:
                while not address:
                    address = await self._address_queue.get_until_empty()

                async with self.async_group.create_subgroup() as subgroup:
                    address_future = subgroup.spawn(
                        self._address_queue.get_until_empty)
                    client_future = subgroup.spawn(self._client_loop, address)

                    await asyncio.wait([address_future, client_future],
                                       return_when=asyncio.FIRST_COMPLETED)

                    if address_future.done():
                        address = address_future.result()

                    elif client_future.done():
                        break

        except Exception as e:
            mlog.error("component address loop error: %s", e, exc_info=e)

        finally:
            self.close()

    async def _client_loop(self, address):
        try:
            while True:
                try:
                    mlog.debug("connecting to server %s", address)
                    client = await connect(address, self._subscriptions)

                except Exception as e:
                    mlog.warning("error connecting to server: %s", e,
                                 exc_info=e)
                    await asyncio.sleep(self._reconnect_delay)
                    continue

                try:
                    mlog.debug("connected to server")
                    async with self.async_group.create_subgroup() as subgroup:
                        client_future = subgroup.spawn(client.wait_closing)
                        runner_future = subgroup.spawn(self._runner_loop,
                                                       client)

                        await asyncio.wait([client_future, runner_future],
                                           return_when=asyncio.FIRST_COMPLETED)

                        if client_future.done():
                            pass

                        elif runner_future.done():
                            break

                finally:
                    await aio.uncancellable(client.async_close())

                mlog.debug("connection to server closed")
                await asyncio.sleep(self._reconnect_delay)

        except Exception as e:
            mlog.error("component client loop error: %s", e, exc_info=e)

    async def _runner_loop(self, client):
        try:
            runner = self._component_cb(client)

            try:
                await runner.wait_closing()

            finally:
                await aio.uncancellable(runner.async_close())

        except Exception as e:
            mlog.error("component runner loop error: %s", e, exc_info=e)
