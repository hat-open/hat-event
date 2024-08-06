import asyncio
import contextlib
import logging
import types
import typing

from hat import aio
from hat import json
from hat.drivers import tcp
import hat.monitor.component

from hat.event import common
from hat.event.server.engine import create_engine
from hat.event.server.eventer_client import create_eventer_client
from hat.event.server.eventer_server import (create_eventer_server,
                                             EventerServer)


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class EventerServerData(typing.NamedTuple):
    server_id: common.ServerId
    addr: tcp.Address


class MainRunner(aio.Resource):

    def __init__(self, conf: json.Data):
        self._conf = conf
        self._loop = asyncio.get_running_loop()
        self._async_group = aio.Group()
        self._backend = None
        self._eventer_server = None
        self._monitor_component = None
        self._eventer_client_runner = None
        self._engine_runner = None

        self.async_group.spawn(self._run)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def _run(self):
        try:
            mlog.debug("starting main runner loop")
            await self._start()

            await self._loop.create_future()

        except Exception as e:
            mlog.error("main runner loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("closing main runner loop")
            self.close()
            await aio.uncancellable(self._stop())

    async def _start(self):
        mlog.debug("creating backend")
        backend_conf = self._conf['backend']
        backend_info = common.import_backend_info(backend_conf['module'])
        self._backend = await aio.call(backend_info.create, backend_conf,
                                       self._on_backend_registered_events,
                                       self._on_backend_flushed_events)
        _bind_resource(self.async_group, self._backend)

        mlog.debug("creating eventer server")
        self._eventer_server = await create_eventer_server(
            addr=tcp.Address(self._conf['eventer_server']['host'],
                             self._conf['eventer_server']['port']),
            backend=self._backend,
            server_id=self._conf['server_id'],
            server_token=self._conf.get('server_token'))
        _bind_resource(self.async_group, self._eventer_server)

        if 'monitor_component' in self._conf:
            mlog.debug("creating monitor component")
            self._monitor_component = await hat.monitor.component.connect(
                addr=tcp.Address(self._conf['monitor_component']['host'],
                                 self._conf['monitor_component']['port']),
                name=self._conf['name'],
                group=self._conf['monitor_component']['group'],
                runner_cb=self._create_monitor_runner,
                data={'server_id': self._conf['server_id'],
                      'eventer_server': self._conf['eventer_server'],
                      'server_token': self._conf.get('server_token')},
                state_cb=self._on_monitor_state)
            _bind_resource(self.async_group, self._monitor_component)

            mlog.debug("creating eventer client runner")
            self._eventer_client_runner = EventerClientRunner(
                conf=self._conf,
                backend=self._backend,
                synced_cb=self._on_eventer_client_synced)
            _bind_resource(self.async_group, self._eventer_client_runner)

            await self._eventer_client_runner.set_monitor_state(
                self._monitor_component.state)

            await self._monitor_component.set_ready(True)

        else:
            mlog.debug("creating engine runner")
            self._engine_runner = EngineRunner(
                conf=self._conf,
                backend=self._backend,
                eventer_server=self._eventer_server)
            _bind_resource(self.async_group, self._engine_runner)

    async def _stop(self):
        if self._engine_runner and not self._monitor_component:
            await self._engine_runner.async_close()

        if self._eventer_client_runner:
            await self._eventer_client_runner.async_close()

        if self._monitor_component:
            await self._monitor_component.async_close()

        if self._eventer_server:
            with contextlib.suppress(Exception):
                await self._backend.flush()

            await self._eventer_server.async_close()

        if self._backend:
            await self._backend.async_close()

    async def _create_monitor_runner(self, monitor_component):
        mlog.debug("creating engine runner")
        self._engine_runner = EngineRunner(conf=self._conf,
                                           backend=self._backend,
                                           eventer_server=self._eventer_server)
        return self._engine_runner

    async def _on_backend_registered_events(self, events):
        if not self._eventer_server:
            return

        await self._eventer_server.notify_events(events, False)

    async def _on_backend_flushed_events(self, events):
        if not self._eventer_server:
            return

        await self._eventer_server.notify_events(events, True)

    async def _on_monitor_state(self, monitor_component, state):
        if not self._eventer_client_runner:
            return

        await self._eventer_client_runner.set_monitor_state(state)

    async def _on_eventer_client_synced(self, server_id, synced, counter):
        if not self._engine_runner:
            return

        await self._engine_runner.set_synced(server_id, synced, counter)


class EventerClientRunner(aio.Resource):

    def __init__(self,
                 conf: json.Data,
                 backend: common.Backend,
                 synced_cb: aio.AsyncCallable[[common.ServerId, bool, int],
                                              None],
                 reconnect_delay: float = 5):
        self._conf = conf
        self._backend = backend
        self._synced_cb = synced_cb
        self._reconnect_delay = reconnect_delay
        self._async_group = aio.Group()
        self._client_subgroups = {}

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def set_monitor_state(self, state: hat.monitor.component.State):
        valid_server_data = set(_get_eventer_server_data(
            group=self._conf['monitor_component']['group'],
            server_token=self._conf.get('server_token'),
            state=state))

        for server_data in list(self._client_subgroups.keys()):
            if server_data in valid_server_data:
                continue

            subgroup = self._client_subgroups.pop(server_data)
            subgroup.close()

        for server_data in valid_server_data:
            subgroup = self._client_subgroups.get(server_data)
            if subgroup and subgroup.is_open:
                continue

            subgroup = self.async_group.create_subgroup()
            subgroup.spawn(self._client_loop, subgroup, server_data)
            self._client_subgroups[server_data] = subgroup

    async def _client_loop(self, async_group, server_data):
        try:
            mlog.debug("staring eventer client runner loop")
            while True:
                try:
                    mlog.debug("creating eventer client")
                    eventer_client = await create_eventer_client(
                        addr=server_data.addr,
                        client_name=self._conf['name'],
                        local_server_id=self._conf['server_id'],
                        remote_server_id=server_data.server_id,
                        backend=self._backend,
                        client_token=self._conf.get('server_token'),
                        synced_cb=self._on_synced)

                except Exception:
                    await asyncio.sleep(self._reconnect_delay)
                    continue

                try:
                    await aio.call(self._synced_cb, server_data.server_id,
                                   False, 0)

                    await eventer_client.wait_closing()

                finally:
                    await aio.uncancellable(eventer_client.async_close())

        except Exception as e:
            mlog.error("eventer client runner loop error: %s", e, exc_info=e)
            self.close()

        finally:
            mlog.debug("closing eventer client runner loop")
            async_group.close()

    async def _on_synced(self, server_id, counter):
        await aio.call(self._synced_cb, server_id, True, counter)


class EngineRunner(aio.Resource):

    def __init__(self,
                 conf: json.Data,
                 backend: common.Backend,
                 eventer_server: EventerServer):
        self._conf = conf
        self._backend = backend
        self._eventer_server = eventer_server
        self._async_group = aio.Group()
        self._engine = None
        self._restart = asyncio.Event()

        self.async_group.spawn(self._run)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def set_synced(self,
                         server_id: common.ServerId,
                         synced: bool,
                         counter: int):
        if self._engine and self._engine.is_open:
            source = common.Source(type=common.SourceType.SERVER, id=0)
            event = common.RegisterEvent(
                type=('event', str(self._conf['server_id']), 'synced',
                      str(server_id)),
                source_timestamp=None,
                payload=common.EventPayloadJson(synced))

            await self._engine.register(source, [event])

        if synced and counter and self._conf.get('synced_restart_engine'):
            self._restart.set()

    async def _run(self):
        try:
            mlog.debug("staring engine runner loop")
            while True:
                self._restart.clear()

                mlog.debug("creating engine")
                self._engine = await create_engine(
                    backend=self._backend,
                    module_confs=self._conf['modules'],
                    server_id=self._conf['server_id'])
                await self._eventer_server.set_engine(self._engine)

                async with self._async_group.create_subgroup() as subgroup:
                    await asyncio.wait(
                        [subgroup.spawn(self._engine.wait_closing),
                         subgroup.spawn(self._restart.wait)],
                        return_when=asyncio.FIRST_COMPLETED)

                if not self._engine.is_open:
                    break

                await self._close()

        except Exception as e:
            mlog.error("engine runner loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("closing engine runner loop")
            self.close()
            await aio.uncancellable(self._close())

    async def _close(self):
        if self._engine:
            await self._engine.async_close()

        await self._eventer_server.set_engine(None)


def _bind_resource(async_group, resource):
    async_group.spawn(aio.call_on_done, resource.wait_closing(),
                      async_group.close)


def _get_eventer_server_data(group, server_token, state):
    for info in state.components:
        if info == state.info or info.group != group:
            continue

        server_id = json.get(info.data, 'server_id')
        host = json.get(info.data, ['eventer_server', 'host'])
        port = json.get(info.data, ['eventer_server', 'port'])
        token = json.get(info.data, 'server_token')
        if (not isinstance(server_id, int) or
                not isinstance(host, str) or
                not isinstance(port, int) or
                not isinstance(token, (str, types.NoneType))):
            continue

        if server_token is not None and token != server_token:
            continue

        yield EventerServerData(server_id=server_id,
                                addr=tcp.Address(host, port))
