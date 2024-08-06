"""Eventer Component"""

from collections.abc import Collection
import asyncio
import contextlib
import logging
import types
import typing

from hat import aio
from hat import json
from hat import util
from hat.drivers import tcp
import hat.monitor.component

from hat.event import common
import hat.event.eventer.client


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

State: typing.TypeAlias = hat.monitor.component.State
"""Component state"""

Runner: typing.TypeAlias = aio.Resource
"""Component runner"""

RunnerCb: typing.TypeAlias = aio.AsyncCallable[
    ['Component', 'ServerData', hat.event.eventer.client.Client],
    Runner]
"""Runner callback"""

StateCb: typing.TypeAlias = aio.AsyncCallable[['Component', State], None]
"""State callback"""

CloseReqCb: typing.TypeAlias = aio.AsyncCallable[['Component'], None]
"""Close request callback"""

StatusCb: typing.TypeAlias = aio.AsyncCallable[
    ['Component', hat.event.eventer.client.Client, common.Status],
    None]
"""Status callback"""

EventsCb: typing.TypeAlias = aio.AsyncCallable[
    ['Component', hat.event.eventer.client.Client, Collection[common.Event]],
    None]
"""Events callback"""


class ServerData(typing.NamedTuple):
    """Server data"""
    server_id: common.ServerId
    addr: tcp.Address
    server_token: str | None


async def connect(addr: tcp.Address,
                  name: str,
                  group: str,
                  server_group: str,
                  runner_cb: RunnerCb,
                  *,
                  state_cb: StateCb | None = None,
                  close_req_cb: CloseReqCb | None = None,
                  status_cb: StatusCb | None = None,
                  events_cb: EventsCb | None = None,
                  server_data_queue_size: int = 1024,
                  reconnect_delay: float = 0.5,
                  observer_kwargs: dict[str, typing.Any] = {},
                  eventer_kwargs: dict[str, typing.Any] = {}
                  ) -> 'Component':
    """Connect to local monitor server and create component

    High-level interface for communication with Event Server, based on
    information obtained from Monitor Server.

    Component instance tries to establish active connection with
    Event Server within monitor component group `server_group`. Once this
    connection is established, `runner_cb` is called with currently active
    eventer client instance. Result of calling `runner_cb` should be runner
    representing user defined components activity associated with connection
    to active Event Server. Once connection to Event Server is closed or new
    active Event Server is detected, associated runner is closed. If new
    connection to Event Server is successfully established,
    `component_cb` will be called again to create new runner associated with
    new instance of eventer client.

    If runner is closed while connection to Event Server is open, component
    is closed.

    """
    component = Component()
    component._name = name
    component._server_group = server_group
    component._runner_cb = runner_cb
    component._state_cb = state_cb
    component._close_req_cb = close_req_cb
    component._status_cb = status_cb
    component._events_cb = events_cb
    component._server_data_queue_size = server_data_queue_size
    component._reconnect_delay = reconnect_delay
    component._eventer_kwargs = eventer_kwargs
    component._server_data = None
    component._server_data_queue = None

    component._monitor_component = await hat.monitor.component.connect(
        addr=addr,
        name=name,
        group=group,
        runner_cb=component._create_monitor_runner,
        **{**observer_kwargs,
           'state_cb': component._on_state,
           'close_req_cb': component._on_close_req})

    return component


class Component(aio.Resource):
    """Eventer Component"""

    @property
    def async_group(self):
        return self._monitor_component.async_group

    @property
    def state(self) -> State:
        """State"""
        return self._monitor_component.state

    @property
    def ready(self) -> bool:
        """Ready"""
        return self._monitor_component.ready

    async def set_ready(self, ready: bool):
        """Set ready"""
        await self._monitor_component.set_ready(ready)

    async def _on_state(self, monitor_component, state):
        if self._server_data_queue is not None:
            data = self._get_active_server_data(state)

            if data != self._server_data:
                self._server_data = data

                with contextlib.suppress(aio.QueueClosedError):
                    await self._server_data_queue.put(data)

        if self._state_cb:
            await aio.call(self._state_cb, self, state)

    async def _on_close_req(self, monitor_component):
        if self._close_req_cb:
            await aio.call(self._close_req_cb, self)

    async def _on_status(self, eventer_client, status):
        if self._status_cb:
            await aio.call(self._status_cb, self, eventer_client, status)

    async def _on_events(self, eventer_client, events):
        if self._events_cb:
            await aio.call(self._events_cb, self, eventer_client, events)

    def _create_monitor_runner(self, monitor_component):
        self._server_data = self._get_active_server_data(
            monitor_component.state)
        self._server_data_queue = aio.Queue(self._server_data_queue_size)

        if self._server_data:
            self._server_data_queue.put_nowait(self._server_data)

        runner = aio.Group()
        runner.spawn(self._server_data_loop, runner, self._server_data_queue)

        return runner

    def _get_active_server_data(self, state):
        info = util.first(state.components, self._active_server_filter)
        if not info:
            return

        server_id = json.get(info.data, 'server_id')
        host = json.get(info.data, ['eventer_server', 'host'])
        port = json.get(info.data, ['eventer_server', 'port'])
        server_token = json.get(info.data, 'server_token')
        if (not isinstance(server_id, int) or
                not isinstance(host, str) or
                not isinstance(port, int) or
                not isinstance(server_token, (str, types.NoneType))):
            return

        client_token = self._eventer_kwargs.get('client_token')
        if client_token is not None and client_token != server_token:
            return

        return ServerData(server_id=server_id,
                          addr=tcp.Address(host, port),
                          server_token=server_token)

    def _active_server_filter(self, info):
        return (info.group == self._server_group and
                info.blessing_req.token is not None and
                info.blessing_req.token == info.blessing_res.token)

    async def _server_data_loop(self, async_group, server_data_queue):
        try:
            server_data = None
            while True:
                while not server_data or not server_data_queue.empty():
                    server_data = await server_data_queue.get_until_empty()

                async with async_group.create_subgroup() as subgroup:
                    subgroup.spawn(self._client_loop, subgroup, server_data)
                    server_data = await server_data_queue.get_until_empty()

        except Exception as e:
            mlog.error("address loop error: %s", e, exc_info=e)
            self.close()

        finally:
            async_group.close()
            server_data_queue.close()

    async def _client_loop(self, async_group, server_data):
        try:
            while True:
                try:
                    kwargs = {**self._eventer_kwargs,
                              'status_cb': self._on_status,
                              'events_cb': self._on_events}

                    if 'server_id' not in kwargs:
                        kwargs['server_id'] = server_data.server_id

                    mlog.debug("connecting to server %s", server_data.addr)
                    client = await hat.event.eventer.client.connect(
                        addr=server_data.addr,
                        client_name=self._name,
                        **kwargs)

                except Exception as e:
                    mlog.warning("error connecting to server: %s", e,
                                 exc_info=e)
                    await asyncio.sleep(self._reconnect_delay)
                    continue

                try:
                    mlog.debug("connected to server")
                    runner = await client.async_group.spawn(
                        aio.call, self._runner_cb, self, server_data, client)

                    try:
                        async with async_group.create_subgroup() as subgroup:
                            client_closing_task = subgroup.spawn(
                                client.wait_closing)
                            runner_closing_task = subgroup.spawn(
                                runner.wait_closing)

                            await asyncio.wait(
                                [client_closing_task, runner_closing_task],
                                return_when=asyncio.FIRST_COMPLETED)

                            if (runner_closing_task.done() and
                                    not client_closing_task.done()):
                                self.close()
                                return

                    finally:
                        await aio.uncancellable(runner.async_close())

                finally:
                    await aio.uncancellable(client.async_close())

                mlog.debug("connection to server closed")
                await asyncio.sleep(self._reconnect_delay)

        except Exception as e:
            mlog.error("client loop error: %s", e, exc_info=e)
            self.close()

        finally:
            async_group.close()
