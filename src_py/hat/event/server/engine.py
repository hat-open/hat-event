"""Engine"""

import asyncio
import collections
import importlib
import logging
import typing

from hat import aio
from hat import json
from hat import util
from hat.event.server import common
from hat.event.server.syncer_server import SyncerServer


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

EventsCb = typing.Callable[[typing.List[common.Event]], None]
"""Events callback"""


async def create_engine(
        conf: json.Data,
        syncer_server: SyncerServer,
        backend: common.Backend
        ) -> 'Engine':
    """Create engine

    Args:
        conf: configuration defined by
            ``hat-event://main.yaml#/definitions/engine``
        syncer_server: syncer server
        backend: backend

    """
    engine = Engine()
    engine._backend = backend
    engine._async_group = aio.Group()
    engine._register_queue = aio.Queue()
    engine._register_cbs = util.CallbackRegistry()

    engine._last_event_id = await backend.get_last_event_id(conf['server_id'])
    if not engine._last_event_id:
        engine._last_event_id = common.EventId(server=conf['server_id'],
                                               session=0,
                                               instance=0)

    engine.register(source=common.Source(type=common.SourceType.ENGINE,
                                         id=0),
                    events=[engine._create_status_event('STARTED')])

    engine._source_modules = collections.deque()
    for source_id, module_conf in enumerate(conf['modules']):
        source = common.Source(type=common.SourceType.MODULE,
                               id=source_id)
        py_module = importlib.import_module(module_conf['module'])
        module = await aio.call(py_module.create, module_conf, engine, source)
        engine.async_group.spawn(aio.call_on_cancel, module.async_close)
        engine._source_modules.append((source, module))

    engine.async_group.spawn(engine._register_loop)

    for _, module in engine._source_modules:
        engine.async_group.spawn(aio.call_on_done, module.wait_closing(),
                                 engine.close)

    return engine


class Engine(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._async_group

    def register_events_cb(self,
                           cb: EventsCb
                           ) -> util.RegisterCallbackHandle:
        """Register events callback"""
        return self._register_cbs.register(cb)

    async def register(self,
                       source: common.Source,
                       events: typing.List[common.RegisterEvent]
                       ) -> typing.List[typing.Optional[common.Event]]:
        """Register events"""
        if not events:
            return []

        future = asyncio.Future()
        self._register_queue.put_nowait((future, source, events))
        return await future

    async def query(self,
                    data: common.QueryData
                    ) -> typing.List[common.Event]:
        """Query events"""
        return await self._backend.query(data)

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

                result = events[:len(register_events)]
                if not future.done():
                    future.set_result(result)

                events = [event for event in events if event]
                if events:
                    self._register_cbs.notify(events)

        except Exception as e:
            mlog.error("register loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("register loop closed")
            self._async_group.close()
            self._register_queue.close()

            while True:
                if future and not future.done():
                    future.set_exception(Exception('module engine closed'))
                if self._register_queue.empty():
                    break
                future, _, __ = self._register_queue.get_nowait()

            events = [self._create_status_event('STOPPED')]
            await self._backend.register(events)

    async def _process_sessions(self, source, register_events):
        timestamp = common.now()
        self._last_event_id = self._last_event_id._replace(
            session=self._last_event_id.session + 1)

        for _, module in self._source_modules:
            await module.start_session(self._last_event_id.session)

        events = collections.deque(
            self._create_event(timestamp, register_event)
            for register_event in register_events)

        input_source_events = [(source, event) for event in events]
        while input_source_events:
            output_source_events = collections.deque()

            for output_source, module in self._source_modules:
                for input_source, input_event in input_source_events:
                    if not module.subscription.matches(input_event.event_type):
                        continue

                    async for register_event in module.process(input_source,
                                                               input_event):
                        output_event = self._create_event(timestamp,
                                                          register_event)
                        output_source_events.append((output_source,
                                                     output_event))
                        events.append(output_event)

            input_source_events = output_source_events

        for _, module in self._source_modules:
            await module.stop_session(self._last_event_id.session)

        return events

    def _create_status_event(self, status):
        self._last_event_id = self._last_event_id._replace(
            session=self._last_event_id.session + 1,
            instance=self._last_event_id.instance + 1)
        return common.Event(
            event_id=self._last_event_id,
            event_type=('event', 'engine'),
            timestamp=common.now(),
            source_timestamp=None,
            payload=common.EventPayload(
                type=common.EventPayloadType.JSON,
                data=status))

    def _create_event(self, timestamp, register_event):
        self._last_event_id = self._last_event_id._replace(
            instance=self._last_event_id.instance + 1)
        return common.Event(event_id=self._last_event_id,
                            event_type=register_event.event_type,
                            timestamp=timestamp,
                            source_timestamp=register_event.source_timestamp,
                            payload=register_event.payload)
