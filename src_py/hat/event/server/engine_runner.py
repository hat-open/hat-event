from collections.abc import Callable
import asyncio
import logging

from hat import aio
from hat import json

from hat.event import common
from hat.event.server.engine import create_engine
from hat.event.server.eventer_client import SyncedState
from hat.event.server.eventer_client_runner import EventerClientRunner
from hat.event.server.eventer_server import EventerServer


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class EngineRunner(aio.Resource):

    def __init__(self,
                 conf: json.Data,
                 backend: common.Backend,
                 eventer_server: EventerServer,
                 eventer_client_runner: EventerClientRunner | None,
                 reset_monitor_ready_cb: Callable[[], None]):
        self._conf = conf
        self._backend = backend
        self._eventer_server = eventer_server
        self._eventer_client_runner = eventer_client_runner
        self._reset_monitor_ready_cb = reset_monitor_ready_cb
        self._async_group = aio.Group()
        self._engine = None
        self._restart = asyncio.Event()

        self.async_group.spawn(self._run)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def set_synced(self,
                         server_id: common.ServerId,
                         state: SyncedState,
                         count: int | None):
        if not self._engine or not self._engine.is_open:
            return

        data = {'state': state.name}
        if state == SyncedState.SYNCED:
            data['count'] = count

        source = common.Source(type=common.SourceType.SERVER, id=0)
        event = common.RegisterEvent(
            type=('event', str(self._conf['server_id']), 'synced',
                  str(server_id)),
            source_timestamp=None,
            payload=common.EventPayloadJson(data))

        await self._engine.register(source, [event])

    async def _run(self):
        try:
            mlog.debug("staring engine runner loop")
            while True:
                await self._wait_while_eventer_client_runner_operational()

                self._restart.clear()

                await self._eventer_server.set_status(common.Status.STARTING,
                                                      None)

                mlog.debug("creating engine")
                self._engine = await create_engine(
                    backend=self._backend,
                    eventer_server=self._eventer_server,
                    module_confs=self._conf['modules'],
                    server_id=self._conf['server_id'],
                    restart_cb=self._restart.set,
                    reset_monitor_ready_cb=self._reset_monitor_ready_cb)
                await self._eventer_server.set_status(
                    common.Status.OPERATIONAL, self._engine)

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
            self._engine.close()

        await self._eventer_server.set_status(common.Status.STOPPING, None)

        if self._engine:
            await self._engine.async_close()

        await self._backend.flush()

        # TODO not needed with wait wile operational
        await self._eventer_server.notify_events([], True, True)

        await self._eventer_server.set_status(common.Status.STANDBY, None)

    async def _wait_while_eventer_client_runner_operational(self):
        if not self._eventer_client_runner:
            return

        while self._eventer_client_runner.operational:
            event = asyncio.Event()
            with self._eventer_client_runner.register_operational_cb(
                    lambda _: event.set()):
                await event.wait()
