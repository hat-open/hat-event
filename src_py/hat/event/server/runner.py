import asyncio
import contextlib
import importlib
import logging

from hat import aio
from hat import json
import hat.monitor.client
import hat.monitor.common

from hat.event.server import common
from hat.event.server.engine import create_engine
from hat.event.server.eventer_server import create_eventer_server
from hat.event.server.mariner_server import create_mariner_server
from hat.event.server.syncer_server import (create_syncer_server,
                                            SyncerServer)
from hat.event.server.syncer_client import (create_syncer_client,
                                            SyncerClientState,
                                            SyncerClient)


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class MainRunner(aio.Resource):

    def __init__(self, conf: json.Data):
        self._conf = conf
        self._async_group = aio.Group()
        self._backend = None
        self._syncer_server = None
        self._mariner_server = None
        self._monitor_client = None
        self._syncer_client = None
        self._monitor_component = None
        self._engine_runner = None

        self.async_group.spawn(self._run)

    @property
    def async_group(self):
        return self._async_group

    async def _run(self):
        try:
            await self._start()
            await asyncio.Future()

        except Exception as e:
            mlog.error("main runner loop error: %s", e, exc_info=e)

        finally:
            self.close()
            await aio.uncancellable(self._stop())

    async def _start(self):
        await self._start_backend()

        if 'syncer_server' in self._conf:
            await self._start_syncer_server()

        if 'mariner_server' in self._conf:
            await self._start_mariner_server()

        if 'monitor' in self._conf:
            await self._start_monitor_client()
            await self._start_syncer_client()
            await self._start_monitor_component()

        else:
            await self._start_engine_runner()

    async def _start_backend(self):
        py_module = importlib.import_module(self._conf['backend']['module'])
        self._backend = await aio.call(py_module.create, self._conf['backend'])

        _bind_resource(self.async_group, self._backend)

    async def _start_syncer_server(self):
        self._syncer_server = await create_syncer_server(
            self._conf['syncer_server'],
            self._backend)

        _bind_resource(self.async_group, self._syncer_server)

    async def _start_mariner_server(self):
        self._mariner_server = await create_mariner_server(
            self._conf['mariner_server'],
            self._backend)

        _bind_resource(self.async_group, self._mariner_server)

    async def _start_monitor_client(self):
        data = {
            'server_id': self._conf['engine']['server_id'],
            'eventer_server_address': self._conf['eventer_server']['address']}

        if 'syncer_server' in self._conf:
            data['syncer_server_address'] = \
                self._conf['syncer_server']['address']

        if 'syncer_token' in self._conf:
            data['syncer_token'] = self._conf['syncer_token']

        self._monitor_client = await hat.monitor.client.connect(
            self._conf['monitor'], data)

        _bind_resource(self.async_group, self._monitor_client)

    async def _start_syncer_client(self):
        self._syncer_client = await create_syncer_client(
            backend=self._backend,
            monitor_client=self._monitor_client,
            monitor_group=self._conf['monitor']['group'],
            name=str(self._conf['engine']['server_id']),
            syncer_token=self._conf.get('syncer_token'))

        _bind_resource(self.async_group, self._syncer_client)

    async def _start_monitor_component(self):

        async def on_run(monitor_component):
            engine_runner = EngineRunner(self._conf, self._backend,
                                         self._syncer_server,
                                         self._syncer_client)

            try:
                await engine_runner.wait_closing()

            finally:
                await aio.uncancellable(engine_runner.async_close())

        self._monitor_component = hat.monitor.client.Component(
            self._monitor_client, on_run)
        self._monitor_component.set_ready(True)

        _bind_resource(self.async_group, self._monitor_component)

    async def _start_engine_runner(self):
        self._engine_runner = EngineRunner(self._conf, self._backend,
                                           self._syncer_server,
                                           self._syncer_client)

        _bind_resource(self.async_group, self._engine_runner)

    async def _stop(self):
        if self._engine_runner:
            await self._engine_runner.async_close()

        if self._monitor_component:
            await self._monitor_component.async_close()

        if self._syncer_client:
            await self._syncer_client.async_close()

        if self._monitor_client:
            await self._monitor_client.async_close()

        if self._mariner_server:
            await self._mariner_server.async_close()

        if self._syncer_server:
            with contextlib.suppress(Exception):
                await self._backend.flush()

            await self._syncer_server.async_close()

        if self._backend:
            await self._backend.async_close()


class EngineRunner(aio.Resource):

    def __init__(self,
                 conf: json.Data,
                 backend: common.Backend,
                 syncer_server: SyncerServer | None,
                 syncer_client: SyncerClient | None):
        self._conf = conf
        self._backend = backend
        self._syncer_server = syncer_server
        self._syncer_client = syncer_client
        self._async_group = aio.Group()
        self._syncer_client_states = {}
        self._engine = None
        self._eventer_server = None
        self._restart_future = None
        self._synced = True

        self.async_group.spawn(self._run)

    @property
    def async_group(self):
        return self._async_group

    async def _run(self):
        try:
            while True:
                subgroup = self.async_group.create_subgroup()

                try:
                    await self._run_engine(subgroup)

                finally:
                    await self._eventer_server.async_close()
                    await self._engine.async_close()

                    await subgroup.async_close()

        except Exception as e:
            mlog.error("engine runner loop error: %s", e, exc_info=e)

        finally:
            self.close()
            await aio.uncancellable(self._stop())

    async def _run_engine(self, subgroup):
        self._engine = await create_engine(self._conf['engine'],
                                           self._backend)
        _bind_resource(subgroup, self._engine)

        with contextlib.ExitStack() as exit_stack:
            if self._syncer_server:
                exit_stack.enter_context(
                    self._syncer_server.register_state_cb(
                        self._on_syncer_server_state))
                self._on_syncer_server_state(self._syncer_server.state)

            self._restart_future = asyncio.Future()
            self._synced = True

            if self._syncer_client:
                exit_stack.enter_context(
                    self._syncer_client.register_state_cb(
                        self._on_syncer_client_state))
                exit_stack.enter_context(
                    self._syncer_client.register_events_cb(
                        self._on_syncer_client_events))

            self._eventer_server = await create_eventer_server(
                self._conf['eventer_server'], self._engine)
            _bind_resource(subgroup, self._eventer_server)

            await subgroup.wrap(self._restart_future)

    async def _stop(self):
        if self._eventer_server:
            await self._eventer_server.async_close()

        if self._engine:
            await self._engine.async_close()

        with contextlib.suppress(Exception):
            await self._backend.flush()

        if self._syncer_server:
            with contextlib.suppress(Exception):
                await self._syncer_server.flush()

    def _on_syncer_server_state(self, client_infos):
        event = common.RegisterEvent(
            event_type=('event', 'syncer', 'server'),
            source_timestamp=None,
            payload=common.EventPayload(
                type=common.EventPayloadType.JSON,
                data=[{'client_name': client_info.name,
                       'synced': client_info.synced}
                      for client_info in client_infos]))

        self.async_group.spawn(self._engine.register, _syncer_source, [event])

    def _on_syncer_client_state(self, server_id, state):
        self._syncer_client_states[server_id] = state
        if state != SyncerClientState.SYNCED:
            return

        async def register_with_restart(events):
            await self._engine.register(_syncer_source, events)
            if self._restart_future and not self._restart_future.done():
                self._restart_future.set_result(None)

        event = common.RegisterEvent(
            event_type=('event', 'syncer', 'client', str(server_id), 'synced'),
            source_timestamp=None,
            payload=common.EventPayload(
                type=common.EventPayloadType.JSON,
                data=True))

        if self._conf.get('synced_restart_engine') and not self._synced:
            self.async_group.spawn(register_with_restart, [event])

        else:
            self.async_group.spawn(self._engine.register, _syncer_source,
                                   [event])

    def _on_syncer_client_events(self, server_id, events):
        state = self._syncer_client_states.pop(server_id, None)
        if state != SyncerClientState.CONNECTED:
            return

        event = common.RegisterEvent(
            event_type=('event', 'syncer', 'client', str(server_id), 'synced'),
            source_timestamp=None,
            payload=common.EventPayload(
                type=common.EventPayloadType.JSON,
                data=False))

        self.async_group.spawn(self._engine.register, _syncer_source, [event])
        self._synced = False


_syncer_source = common.Source(type=common.SourceType.SYNCER,
                               id=0)


def _bind_resource(async_group, resource):
    async_group.spawn(aio.call_on_done, resource.wait_closing(),
                      async_group.close)
