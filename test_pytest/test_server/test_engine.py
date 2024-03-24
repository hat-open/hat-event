import itertools
import sys
import types

import pytest

from hat import aio

from hat.event import common
import hat.event.server.engine


class Backend(common.Backend):

    def __init__(self, register_cb=None, query_cb=None):
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
            await aio.call(self._register_cb, events)

        return events

    async def query(self, params):
        if not self._query_cb:
            return common.QueryResult([], False)

        return await aio.call(self._query_cb, params)

    async def flush(self):
        pass


def assert_event(event, register_event):
    assert event.type == register_event.type
    assert event.source_timestamp == register_event.source_timestamp
    assert event.payload == register_event.payload


@pytest.fixture
def create_module():
    module_names = set()

    def create_module(create_cb=None, session_start_cb=None,
                      session_stop_cb=None, process_cb=None):

        async def create(conf, engine, source):
            if create_cb:
                await aio.call(create_cb, conf, engine, source)

            module = Module()
            module._async_group = aio.Group()
            module._subscription = common.create_subscription([('*', )])

            return module

        class Module(common.Module):

            @property
            def async_group(self):
                return self._async_group

            @property
            def subscription(self):
                return self._subscription

            async def process(self, source, event):
                if not process_cb:
                    return

                return await aio.call(process_cb, source, event)

            async def on_session_start(self, session_id):
                if not session_start_cb:
                    return

                await aio.call(session_start_cb, session_id)

            async def on_session_stop(self, session_id):
                if not session_stop_cb:
                    return

                await aio.call(session_stop_cb, session_id)

        for i in itertools.count(1):
            module_name = f'mock_module_{i}'
            if module_name not in sys.modules:
                break

        py_module = types.ModuleType(module_name)
        py_module.info = common.ModuleInfo(create)
        sys.modules[module_name] = py_module

        module_names.add(module_name)
        return module_name

    try:
        yield create_module

    finally:
        for module_name in module_names:
            del sys.modules[module_name]


async def test_create_engine():
    backend = Backend()
    engine = await hat.event.server.engine.create_engine(backend, [], 1)

    assert engine.is_open

    await engine.async_close()
    await backend.async_close()


@pytest.mark.parametrize('server_id', [1, 42])
async def test_engine_events(server_id):
    events_queue = aio.Queue()

    def on_register(events):
        events_queue.put_nowait(events)
        return events

    backend = Backend(register_cb=on_register)
    engine = await hat.event.server.engine.create_engine(backend, [],
                                                         server_id)

    events = await events_queue.get()
    assert len(events) == 1
    event = events[0]

    assert event.id == common.EventId(server_id, 1, 1)
    assert event.type == ('event', str(server_id), 'engine')
    assert event.source_timestamp is None
    assert event.payload.data == 'STARTED'

    await engine.async_close()

    events = await events_queue.get()
    assert len(events) == 1
    event = events[0]

    assert event.id == common.EventId(server_id, 2, 1)
    assert event.type == ('event', str(server_id), 'engine')
    assert event.source_timestamp is None
    assert event.payload.data == 'STOPPED'

    await backend.async_close()


@pytest.mark.parametrize('server_id', [1, 42])
@pytest.mark.parametrize('modules_count', [1, 5])
async def test_create_module(server_id, modules_count, create_module):
    index = 0
    sources = set()

    def on_create(conf, engine, source):
        nonlocal index
        assert conf['index'] == index
        sources.add(source)
        index += 1

    modules = [create_module(create_cb=on_create)
               for _ in range(modules_count)]
    confs = [{'module': module,
              'index': i}
             for i, module in enumerate(modules)]
    backend = Backend()
    engine = await hat.event.server.engine.create_engine(backend, confs,
                                                         server_id)

    assert engine.is_open
    assert len(sources) == modules_count

    await engine.async_close()
    await backend.async_close()


@pytest.mark.parametrize('query_params', [common.QueryLatestParams(),
                                          common.QueryTimeseriesParams()])
@pytest.mark.parametrize('query_result', [common.QueryResult([], False)])
async def test_query(query_params, query_result):

    def on_query(params):
        assert params == query_params
        return query_result

    backend = Backend(query_cb=on_query)
    engine = await hat.event.server.engine.create_engine(backend, [], 1)

    result = await engine.query(query_params)
    assert result == query_result

    await engine.async_close()
    await backend.async_close()


async def test_register(create_module):
    eventer_source = common.Source(common.SourceType.EVENTER, 123)
    eventer_register_event = common.RegisterEvent(
        type=('a', 'b', 'c'),
        source_timestamp=common.now(),
        payload=common.EventPayloadJson(42))
    module_register_event = common.RegisterEvent(
        type=('x', 'y', 'z'),
        source_timestamp=None,
        payload=common.EventPayloadBinary('type', b'42'))

    source_event_queue = aio.Queue()

    def on_process(source, event):
        source_event_queue.put_nowait((source, event))

        if source == eventer_source:
            yield module_register_event

    module = create_module(process_cb=on_process)
    confs = [{'module': module}]
    backend = Backend()
    engine = await hat.event.server.engine.create_engine(backend, confs, 1)

    source, event = await source_event_queue.get()
    assert source.type == common.SourceType.ENGINE
    assert event.type == ('event', '1', 'engine')
    assert event.payload.data == 'STARTED'

    assert source_event_queue.empty()

    result = await engine.register(eventer_source, [eventer_register_event])
    assert len(result) == 1
    assert_event(result[0], eventer_register_event)

    source, event = await source_event_queue.get()
    assert source == eventer_source
    assert_event(event, eventer_register_event)

    session_id = event.id.session
    timestamp = event.timestamp

    source, event = await source_event_queue.get()
    assert source.type == common.SourceType.MODULE
    assert_event(event, module_register_event)

    assert event.id.session == session_id
    assert event.timestamp == timestamp

    await engine.async_close()
    await backend.async_close()

    assert source_event_queue.empty()


async def test_session_start_stop(create_module):
    eventer_source = common.Source(common.SourceType.EVENTER, 123)
    eventer_register_event = common.RegisterEvent(type=('a', 'b', 'c'),
                                                  source_timestamp=None,
                                                  payload=None)

    session_start_queue = aio.Queue()
    session_stop_queue = aio.Queue()

    module = create_module(session_start_cb=session_start_queue.put_nowait,
                           session_stop_cb=session_stop_queue.put_nowait)
    confs = [{'module': module}]
    backend = Backend()
    engine = await hat.event.server.engine.create_engine(backend, confs, 1)

    session_id = await session_start_queue.get()
    assert session_id == 1

    session_id = await session_stop_queue.get()
    assert session_id == 1

    assert session_start_queue.empty()
    assert session_stop_queue.empty()

    await engine.register(eventer_source, [eventer_register_event])

    session_id = await session_start_queue.get()
    assert session_id == 2

    session_id = await session_stop_queue.get()
    assert session_id == 2

    await engine.async_close()
    await backend.async_close()

    assert session_start_queue.empty()
    assert session_stop_queue.empty()
