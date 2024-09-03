import asyncio
import contextlib
import functools
import logging

from hat import aio
from hat import json
from hat.drivers import tcp
import hat.monitor.component

from hat.event import common
from hat.event.server.engine_runner import EngineRunner
from hat.event.server.eventer_client_runner import EventerClientRunner
from hat.event.server.eventer_server import create_eventer_server


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class MainRunner(aio.Resource):

    def __init__(self,
                 conf: json.Data,
                 reset_monitor_ready_timeout: float = 30):
        self._conf = conf
        self._reset_monitor_ready_timeout = reset_monitor_ready_timeout
        self._loop = asyncio.get_running_loop()
        self._async_group = aio.Group()
        self._backend = None
        self._eventer_server = None
        self._monitor_component = None
        self._eventer_client_runner = None
        self._engine_runner = None
        self._reset_monitor_ready_start = asyncio.Event()
        self._reset_monitor_ready_stop = asyncio.Event()

        self.async_group.spawn(self._run)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def _run(self):
        try:
            mlog.debug("starting main runner loop")
            await self._start()

            if not self._monitor_component:
                await self._loop.create_future()
                return

            await self._monitor_component.set_ready(True)

            while True:
                self._reset_monitor_ready_start.clear()
                await self._reset_monitor_ready_start.wait()

                await self._monitor_component.set_ready(False)

                if (self._monitor_component.state.info and
                        self._monitor_component.state.info.blessing_res.token):
                    self._reset_monitor_ready_stop.clear()

                    with contextlib.suppress(asyncio.TimeoutError):
                        await aio.wait_for(
                            self._reset_monitor_ready_stop.wait(),
                            self._reset_monitor_ready_timeout)

                await self._monitor_component.set_ready(True)

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
        self._backend = await aio.call(
            backend_info.create, backend_conf,
            functools.partial(self._on_backend_events, False),
            functools.partial(self._on_backend_events, True))
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

        else:
            mlog.debug("creating engine runner")
            self._engine_runner = EngineRunner(
                conf=self._conf,
                backend=self._backend,
                eventer_server=self._eventer_server,
                reset_monitor_ready_cb=self._reset_monitor_ready_start.set)
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

    def _create_monitor_runner(self, monitor_component):
        mlog.debug("creating engine runner")
        self._engine_runner = EngineRunner(
            conf=self._conf,
            backend=self._backend,
            eventer_server=self._eventer_server,
            reset_monitor_ready_cb=self._reset_monitor_ready_start.set)
        return self._engine_runner

    async def _on_backend_events(self, persisted, events):
        if not self._eventer_server:
            return

        await self._eventer_server.notify_events(events, persisted)

    def _on_monitor_state(self, monitor_component, state):
        if not state.info or state.info.blessing_res.token is None:
            self._reset_monitor_ready_stop.set()

        if not self._eventer_client_runner:
            return

        self._eventer_client_runner.set_monitor_state(state)

    async def _on_eventer_client_synced(self, server_id, state, count):
        if not self._engine_runner:
            return

        await self._engine_runner.set_synced(server_id, state, count)


def _bind_resource(async_group, resource):
    async_group.spawn(aio.call_on_done, resource.wait_closing(),
                      async_group.close)
