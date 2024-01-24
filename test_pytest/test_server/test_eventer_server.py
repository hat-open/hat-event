import asyncio
import itertools

import pytest

from hat import aio
from hat import util
from hat.drivers import tcp

from hat.event.server import common
import hat.event.eventer
import hat.event.server.eventer_server


class Backend(common.Backend):

    def __init__(self, query_cb=None):
        self._query_cb = query_cb
        self._async_group = aio.Group()

    @property
    def async_group(self):
        return self._async_group

    async def get_last_event_id(self, server_id):
        raise NotImplementedError()

    async def register(self, events):
        raise NotImplementedError()

    async def query(self, params):
        if not self._query_cb:
            return common.QueryResult(events=[],
                                      more_follows=False)

        return await aio.call(self._query_cb, params)

    async def flush(self):
        raise NotImplementedError()


class Engine(common.Engine):

    def __init__(self, register_cb=None):
        self._register_cb = register_cb
        self._async_group = aio.Group()
        self._next_session_ids = itertools.count(1)

    @property
    def async_group(self):
        return self._async_group

    async def register(self, source, events):
        if self._register_cb:
            await aio.call(self._register_cb, source, events)

        session_id = next(self._next_session_ids)
        timestamp = common.now()

        return [common.Event(id=common.EventId(server=1,
                                               session=session_id,
                                               instance=i+1),
                             type=event.type,
                             timestamp=timestamp,
                             source_timestamp=event.source_timestamp,
                             payload=event.payload)
                for i, event in enumerate(events)]

    async def query(self, params):
        raise NotImplementedError()


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_create(addr):
    backend = Backend()
    server = await hat.event.server.eventer_server.create_eventer_server(
        addr=addr,
        backend=backend)

    assert server.is_open

    await server.async_close()
    await backend.async_close()


@pytest.mark.parametrize('token', [None, 'token'])
async def test_connect(token, addr):
    backend = Backend()
    server = await hat.event.server.eventer_server.create_eventer_server(
        addr=addr,
        backend=backend,
        server_token=token)

    client = await hat.event.eventer.connect(addr=addr,
                                             client_name='client',
                                             client_token=token)
    assert client.is_open
    await client.async_close()

    client = await hat.event.eventer.connect(addr=addr,
                                             client_name='client',
                                             client_token=None)
    assert client.is_open
    await client.async_close()

    await server.async_close()
    await backend.async_close()


async def test_invalid_token(addr):
    backend = Backend()
    server = await hat.event.server.eventer_server.create_eventer_server(
        addr=addr,
        backend=backend,
        server_token='token1')

    with pytest.raises(hat.event.eventer.EventerInitError):
        await hat.event.eventer.connect(addr=addr,
                                        client_name='client',
                                        client_token='token2')

    await server.async_close()
    await backend.async_close()


async def test_set_engine(addr):
    status_queue = aio.Queue()

    def on_status(client, status):
        status_queue.put_nowait(status)

    backend = Backend()
    engine = Engine()
    server = await hat.event.server.eventer_server.create_eventer_server(
        addr=addr,
        backend=backend)
    client = await hat.event.eventer.connect(addr=addr,
                                             client_name='client',
                                             status_cb=on_status)

    assert client.status == common.Status.STANDBY

    await server.set_engine(engine)

    status = await status_queue.get()
    assert status == common.Status.OPERATIONAL
    assert status_queue.empty()

    await server.set_engine(None)

    status = await status_queue.get()
    assert status == common.Status.STANDBY
    assert status_queue.empty()

    await client.async_close()
    await server.async_close()
    await engine.async_close()
    await backend.async_close()


async def test_register(addr):
    client_name = 'client name'
    register_queue = aio.Queue()

    def on_register(source, events):
        assert source.type == common.SourceType.EVENTER
        register_queue.put_nowait(events)

    backend = Backend()
    engine = Engine(register_cb=on_register)
    server = await hat.event.server.eventer_server.create_eventer_server(
        addr=addr,
        backend=backend)
    await server.set_engine(engine)

    client = await hat.event.eventer.connect(addr=addr,
                                             client_name=client_name)
    assert client.status == common.Status.OPERATIONAL

    events = await register_queue.get()
    assert len(events) == 1
    event = events[0]
    assert event.type == ('event', 'eventer', client_name)
    assert event.source_timestamp is None
    assert event.payload.data == 'CONNECTED'

    event = common.RegisterEvent(type=('a', 'b', 'c'),
                                 source_timestamp=common.now(),
                                 payload=common.EventPayloadJson('abc'))
    await client.register([event])

    events = await register_queue.get()
    assert events == [event]

    await client.async_close()

    events = await register_queue.get()
    assert len(events) == 1
    event = events[0]
    assert event.type == ('event', 'eventer', client_name)
    assert event.source_timestamp is None
    assert event.payload.data == 'DISCONNECTED'

    await server.async_close()
    await engine.async_close()
    await backend.async_close()


@pytest.mark.parametrize('events_count', [1, 5])
@pytest.mark.parametrize('persisted', [True, False])
async def test_notify_events(events_count, persisted, addr):
    events = [common.Event(id=common.EventId(server=1,
                                             session=123,
                                             instance=i+1),
                           type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=common.EventPayloadJson(i))
              for i in range(events_count)]
    events_queue = aio.Queue()

    def on_events(client, events):
        events_queue.put_nowait(events)

    backend = Backend()
    server = await hat.event.server.eventer_server.create_eventer_server(
        addr=addr,
        backend=backend)
    client = await hat.event.eventer.connect(addr=addr,
                                             client_name='client',
                                             subscriptions=[('*', )],
                                             persisted=persisted,
                                             events_cb=on_events)

    await server.notify_events(events, persisted)

    result = await events_queue.get()
    assert result == events

    await server.notify_events(events, not persisted)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(events_queue.get(), 0.01)

    await client.async_close()
    await server.async_close()
    await backend.async_close()


async def test_query(addr):
    params = common.QueryLatestParams()
    result = common.QueryResult(events=[], more_follows=False)

    def on_query(p):
        assert p == params
        return result

    backend = Backend(query_cb=on_query)
    server = await hat.event.server.eventer_server.create_eventer_server(
        addr=addr,
        backend=backend)
    client = await hat.event.eventer.connect(addr=addr,
                                             client_name='client')

    res = await client.query(params)
    assert res == result

    await client.async_close()
    await server.async_close()
    await backend.async_close()
