import asyncio
import itertools
import sys
import types

import pytest

from hat import aio
from hat.event.server import common
from hat.event.server.engine import create_engine


@pytest.fixture
def create_module():
    module_names = []

    def create_module(process_cb, session_start_cb=None, session_stop_cb=None):

        async def create(conf, engine, source):
            module = Module()
            module._async_group = aio.Group()
            module._subscription = common.Subscription([['*']])
            module._source = source
            return module

        class Module(common.Module):

            @property
            def async_group(self):
                return self._async_group

            @property
            def subscription(self):
                return self._subscription

            async def process(self, source, event):
                async for i in process_cb(source, event, self._source):
                    yield i

            async def on_session_start(self,
                                       session_id: int):
                if session_start_cb:
                    await aio.call(session_start_cb, session_id)

            async def on_session_stop(self,
                                      session_id: int):
                if session_stop_cb:
                    await aio.call(session_stop_cb, session_id)

        for i in itertools.count(1):
            module_name = f'mock_module_{i}'
            if module_name not in sys.modules:
                break

        module = types.ModuleType(module_name)
        module.json_schema_id = None
        module.json_schema_repo = None
        module.create = create
        sys.modules[module_name] = module

        module_names.append(module_name)
        return module_name

    try:
        yield create_module
    finally:
        for module_name in module_names:
            del sys.modules[module_name]


class Backend(aio.Resource):

    def __init__(self, register_cb=None, query_cb=None, server_id=0):
        self._register_cb = register_cb
        self._query_cb = query_cb
        self._async_group = aio.Group()

    @property
    def async_group(self):
        return self._async_group

    async def get_last_event_id(self, server_id):
        return common.EventId(server=server_id, session=0, instance=0)

    async def register(self, events):
        if self._register_cb:
            self._register_cb(events)
        return events

    async def query(self, data):
        if not self._query_cb:
            return []
        return self._query_cb(data)


async def test_create():
    events_q = aio.Queue()

    def on_backend_events(events):
        events_q.put_nowait(events)
        return events

    conf = {'server_id': 123,
            'modules': []}
    backend = Backend(register_cb=on_backend_events)

    engine = await create_engine(conf, backend)
    assert engine.is_open

    events = await events_q.get()
    assert len(events) == 1
    engine_event = events[0]
    assert engine_event.event_type == ('event', 'engine')
    assert engine_event.payload.data == 'STARTED'

    engine.close()
    await engine.wait_closing()
    assert not engine.is_open
    assert engine.is_closing
    await engine.wait_closed()
    assert engine.is_closed

    events = await events_q.get()
    engine_event = events[0]
    assert engine_event.event_type == ('event', 'engine')
    assert engine_event.payload.data == 'STOPPED'

    await backend.async_close()


@pytest.mark.parametrize("module_count", [0, 1, 2, 5])
@pytest.mark.parametrize("value", [0, 1, 2, 5])
async def test_register(create_module, module_count, value):

    backend_events_q = aio.Queue()
    engine_events_q = aio.Queue()
    started_sessions = []
    stopped_sessions = []
    module_sources = []
    sources = []

    def on_backend_events(events):
        backend_events_q.put_nowait(events)
        return events

    def on_engine_events(events):
        engine_events_q.put_nowait(events)

    async def process(source, event, module_source):
        sources.append(source)
        if source.type == common.SourceType.ENGINE:
            return
        if source.type == common.SourceType.MODULE:
            if source not in module_sources:
                module_sources.append(source)
            if source != module_source:
                return
        if not event.payload.data:
            return
        yield common.RegisterEvent(
            event_type=event.event_type,
            source_timestamp=None,
            payload=event.payload._replace(data=event.payload.data - 1))

    async def session_start(session_id):
        started_sessions.append(session_id)

    async def session_stop(session_id):
        stopped_sessions.append(session_id)

    module_names = [create_module(process, session_start, session_stop)
                    for i in range(module_count)]

    server_id = 12345
    conf = {'server_id': server_id,
            'modules': [{'module': module_name}
                        for module_name in module_names]}
    backend = Backend(register_cb=on_backend_events)

    engine = await create_engine(conf, backend)
    engine.register_events_cb(on_engine_events)

    backend_events = await backend_events_q.get()
    engine_events = await engine_events_q.get()
    assert backend_events == engine_events

    source_eventer = common.Source(common.SourceType.EVENTER, 0)
    reg_events = [common.RegisterEvent(
        event_type=(),
        source_timestamp=None,
        payload=common.EventPayload(common.EventPayloadType.JSON,  value))]
    result = await engine.register(source_eventer, reg_events)

    assert len(result) == 1
    res_event = result[0]
    assert res_event.timestamp
    assert res_event.event_id.server == server_id
    assert res_event.payload.data == value

    timestamp = res_event.timestamp
    session = res_event.event_id.session

    backend_events = backend_events_q.get_nowait()
    engine_events = engine_events_q.get_nowait()
    assert backend_events == engine_events

    events = backend_events
    assert all(e.timestamp == timestamp for e in events)
    assert all(e.event_id.session == session for e in events)
    assert all(e.event_id.server == server_id for e in events)
    assert len(set(e.event_id.instance for e in events)) == len(events)

    values_exp = [value]
    if module_count:
        v = value
        for i in range(value):
            v -= 1
            values_exp.extend([v] * module_count)
            if not v:
                break
    assert [event.payload.data for event in events] == values_exp

    if module_count:
        sources_exp = (
            [common.Source(common.SourceType.ENGINE, 0)] * module_count +
            [source_eventer] * module_count +
            module_sources * (len(values_exp) - 1))
        assert sources_exp == sources

    if module_count:
        sessions_exp = [1] * module_count + [2] * module_count
        assert started_sessions == sessions_exp
        assert stopped_sessions == sessions_exp

    # another register -> another session
    result2 = await engine.register(source_eventer, reg_events)
    res_event2 = result2[0]
    assert res_event2.timestamp > res_event.timestamp
    assert res_event2.event_id.server == server_id
    assert res_event2.event_id.session > session
    assert res_event2.event_id.instance > res_event.event_id.instance

    session2 = res_event2.event_id.session
    events2 = backend_events_q.get_nowait()
    assert len(events2) == len(events)
    assert all(e.event_id.session == session2 for e in events2)
    instances = set(e.event_id.instance for e in itertools.chain(events,
                                                                 events2))
    assert len(instances) == len(events) + len(events2)

    if module_count:
        sessions_exp += [3] * module_count
        assert started_sessions == sessions_exp
        assert stopped_sessions == sessions_exp

    await engine.async_close()
    await backend.async_close()


async def test_query():
    query_data = common.QueryData()
    server_id = 1
    events = [common.Event(event_id=common.EventId(server_id, 2, i),
                           event_type=(),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(10)]

    def on_query(data):
        assert data == query_data
        return events

    conf = {'server_id': server_id, 'modules': []}
    backend = Backend(query_cb=on_query)
    engine = await create_engine(conf, backend)

    result = await engine.query(query_data)
    assert result == events

    await engine.async_close()
    await backend.async_close()


async def test_cancel_register(create_module):

    process_enter_future = asyncio.Future()
    process_exit_future = asyncio.Future()

    async def process(source, event, module_source):
        if source.type != common.SourceType.EVENTER:
            return
        process_enter_future.set_result(None)
        await process_exit_future
        return
        yield

    module_name = create_module(process)

    conf = {'server_id': 1, 'modules': [{'module': module_name}]}
    backend = Backend()

    engine = await create_engine(conf, backend)
    registered_events = aio.Queue()
    engine.register_events_cb(registered_events.put_nowait)
    await registered_events.get()  # engine event

    source = common.Source(common.SourceType.EVENTER, 0)
    register_future = asyncio.ensure_future(
        engine.register(source, [common.RegisterEvent(
            event_type=(),
            source_timestamp=None,
            payload=None)]))

    await process_enter_future
    register_future.cancel()
    process_exit_future.set_result(None)

    assert registered_events.empty()
    await asyncio.wait_for(registered_events.get(), 0.01)

    assert engine.is_open

    await engine.async_close()
