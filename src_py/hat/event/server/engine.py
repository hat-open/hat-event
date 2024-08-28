"""Engine"""

from collections.abc import Callable, Collection, Iterable
import asyncio
import collections
import logging

from hat import aio
from hat import json

from hat.event import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


async def create_engine(backend: common.Backend,
                        module_confs: Iterable[json.Data],
                        server_id: int,
                        restart_cb: Callable[[], None],
                        reset_monitor_ready_cb: Callable[[], None],
                        register_queue_size: int = 1024
                        ) -> 'Engine':
    """Create engine"""
    engine = Engine()
    engine._backend = backend
    engine._server_id = server_id
    engine._restart_cb = restart_cb
    engine._reset_monitor_ready_cb = reset_monitor_ready_cb
    engine._loop = asyncio.get_running_loop()
    engine._async_group = aio.Group()
    engine._register_queue = aio.Queue(register_queue_size)
    engine._source_modules = collections.deque()

    engine._last_event_id = await backend.get_last_event_id(server_id)

    future = engine._loop.create_future()
    source = common.Source(type=common.SourceType.ENGINE, id=0)
    events = [engine._create_status_reg_event('STARTED')]
    engine._register_queue.put_nowait((future, source, events))

    try:
        for source_id, module_conf in enumerate(module_confs):
            info = common.import_module_info(module_conf['module'])
            source = common.Source(type=common.SourceType.MODULE,
                                   id=source_id)

            module = await engine.async_group.spawn(
                aio.call, info.create, module_conf, engine, source)
            engine.async_group.spawn(aio.call_on_cancel, module.async_close)
            engine.async_group.spawn(aio.call_on_done, module.wait_closing(),
                                     engine.close)

            engine._source_modules.append((source, module))

        engine.async_group.spawn(engine._register_loop)

    except BaseException:
        await aio.uncancellable(engine.async_close())
        raise

    return engine


class Engine(common.Engine):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._async_group

    @property
    def server_id(self) -> int:
        return self._server_id

    async def register(self,
                       source: common.Source,
                       events: Collection[common.RegisterEvent]
                       ) -> Collection[common.Event] | None:
        if not events:
            return []

        future = self._loop.create_future()

        try:
            await self._register_queue.put((future, source, events))

        except aio.QueueClosedError:
            raise Exception('engine closed')

        return await future

    async def query(self,
                    params: common.QueryParams
                    ) -> common.QueryResult:
        return await self._backend.query(params)

    def restart(self):
        self._restart_cb()

    def reset_monitor_ready(self):
        self._reset_monitor_ready_cb()

    async def _register_loop(self):
        future = None
        mlog.debug("starting register loop")

        try:
            while True:
                mlog.debug("waiting for register requests")
                future, source, register_events = \
                    await self._register_queue.get()

                mlog.debug("processing session")
                events = await self._process_sessions(source, register_events)

                mlog.debug("registering to backend")
                events = await self._backend.register(events)

                if future.done():
                    continue

                result = (
                    list(event for event, _ in zip(events, register_events))
                    if events is not None else None)
                future.set_result(result)

        except Exception as e:
            mlog.error("register loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("register loop closed")
            self.close()
            self._register_queue.close()

            while True:
                if future and not future.done():
                    future.set_exception(Exception('engine closed'))
                if self._register_queue.empty():
                    break
                future, _, __ = self._register_queue.get_nowait()

            timestamp = self._create_session()
            status_reg_event = self._create_status_reg_event('STOPPED')
            events = [self._create_event(timestamp, status_reg_event)]
            await self._backend.register(events)

    async def _process_sessions(self, source, register_events):
        timestamp = self._create_session()

        for _, module in self._source_modules:
            await aio.call(module.on_session_start,
                           self._last_event_id.session)

        events = collections.deque(
            self._create_event(timestamp, register_event)
            for register_event in register_events)

        input_source_events = [(source, event) for event in events]
        while input_source_events:
            output_source_events = collections.deque()

            for output_source, module in self._source_modules:
                for input_source, input_event in input_source_events:
                    if not module.subscription.matches(input_event.type):
                        continue

                    output_register_events = await aio.call(
                        module.process, input_source, input_event)

                    if not output_register_events:
                        continue

                    for output_register_event in output_register_events:
                        output_event = self._create_event(
                            timestamp, output_register_event)
                        output_source_events.append(
                            (output_source, output_event))
                        events.append(output_event)

            input_source_events = output_source_events

        for _, module in self._source_modules:
            await aio.call(module.on_session_stop, self._last_event_id.session)

        return events

    def _create_status_reg_event(self, status):
        return common.RegisterEvent(
            type=('event', str(self._server_id), 'engine'),
            source_timestamp=None,
            payload=common.EventPayloadJson(status))

    def _create_session(self):
        self._last_event_id = self._last_event_id._replace(
            session=self._last_event_id.session + 1,
            instance=0)

        return common.now()

    def _create_event(self, timestamp, register_event):
        self._last_event_id = self._last_event_id._replace(
            instance=self._last_event_id.instance + 1)

        return common.Event(id=self._last_event_id,
                            type=register_event.type,
                            timestamp=timestamp,
                            source_timestamp=register_event.source_timestamp,
                            payload=register_event.payload)
