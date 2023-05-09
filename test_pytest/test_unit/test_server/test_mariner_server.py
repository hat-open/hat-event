import asyncio
import collections

import pytest

from hat import aio
from hat import util
from hat.drivers import tcp

from hat.event import mariner
from hat.event.server import common
from hat.event.server.mariner_server import create_mariner_server


@pytest.fixture
def port():
    return util.get_unused_tcp_port()


@pytest.fixture
def address(port):
    return tcp.Address('127.0.0.1', port)


@pytest.fixture
def conf(address):
    return {'address': f'tcp://{address.host}:{address.port}',
            'subscriptions': [['*']]}


class Backend(aio.Resource):

    def __init__(self, query_cb=None):
        self._query_cb = query_cb
        self._async_group = aio.Group()
        self._events_cbs = util.CallbackRegistry()

    @property
    def async_group(self):
        return self._async_group

    def register_registered_events_cb(self, cb):
        return self._events_cbs.register(cb)

    def register_flushed_events_cb(self, cb):
        return self._events_cbs.register(cb)

    async def get_last_event_id(self, server_id):
        raise NotImplementedError()

    async def register(self, events):
        self._events_cbs.notify(events)
        return events

    async def query(self, data):
        raise NotImplementedError()

    async def query_flushed(self, event_id):
        if self._query_cb:
            async for events in self._query_cb(event_id):
                yield events

    async def flush(self):
        raise NotImplementedError()


async def test_create(conf):
    backend = Backend()
    server = await create_mariner_server(conf, backend)

    assert server.is_open

    await server.async_close()
    await backend.async_close()


@pytest.mark.parametrize("client_count", [1, 2, 5])
async def test_connect(conf, address, client_count):
    backend = Backend()
    server = await create_mariner_server(conf, backend)

    clients = collections.deque()
    for i in range(client_count):
        client = await mariner.connect(address, f'client {i}')
        clients.append(client)

    while clients:
        client = clients.popleft()
        assert client.is_open
        await client.async_close()

    await server.async_close()
    await backend.async_close()


async def test_query(conf, address):
    last_event_id = common.EventId(1, 2, 3)
    events = [common.Event(event_id=common.EventId(1, 2, 4 + i),
                           event_type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(3)]

    async def on_query(event_id):
        assert event_id == last_event_id
        yield events

    backend = Backend(on_query)
    server = await create_mariner_server(conf, backend)

    events_queue = aio.Queue()
    client = await mariner.connect(address, 'client id',
                                   last_event_id=last_event_id,
                                   subscriptions=[('*', )],
                                   events_cb=events_queue.put_nowait)

    result = await events_queue.get()
    assert result == events

    await client.async_close()
    await server.async_close()
    await backend.async_close()


async def test_events(conf, address):
    query_future = asyncio.Future()
    last_event_id = common.EventId(1, 0, 0)
    events = [common.Event(event_id=common.EventId(1, 1, 1 + i),
                           event_type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(3)]

    async def on_query(event_id):
        query_future.set_result(None)
        yield []

    backend = Backend(on_query)
    server = await create_mariner_server(conf, backend)

    events_queue = aio.Queue()
    client = await mariner.connect(address, 'client id',
                                   subscriptions=[('*', )],
                                   last_event_id=last_event_id,
                                   events_cb=events_queue.put_nowait)

    await query_future

    assert events_queue.empty()

    await backend.register(events)

    result = await events_queue.get()
    assert result == events

    await client.async_close()
    await server.async_close()
    await backend.async_close()


async def test_subscription(conf, address):
    query_future = asyncio.Future()
    last_event_id = common.EventId(1, 0, 0)
    events = [common.Event(event_id=common.EventId(1, 1, 1),
                           event_type=('a', ),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None),
              common.Event(event_id=common.EventId(1, 1, 2),
                           event_type=('b', ),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)]

    async def on_query(event_id):
        query_future.set_result(None)
        yield []

    backend = Backend(on_query)
    server = await create_mariner_server(conf, backend)

    events_queue = aio.Queue()
    client = await mariner.connect(address, 'client id',
                                   subscriptions=[('a', '*')],
                                   last_event_id=last_event_id,
                                   events_cb=events_queue.put_nowait)

    await query_future

    assert events_queue.empty()

    await backend.register(events)

    result = await events_queue.get()
    assert result == [events[0]]

    await client.async_close()
    await server.async_close()
    await backend.async_close()
