import asyncio

import pytest

from hat import aio
from hat import util
from hat.drivers import tcp

from hat.event import common
import hat.event.server.eventer_client
import hat.event.eventer.server


class Backend(common.Backend):

    def __init__(self, register_cb=None):
        self._register_cb = register_cb
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
        raise NotImplementedError()

    async def flush(self):
        raise NotImplementedError()


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_create(addr):
    backend = Backend()
    server = await hat.event.eventer.server.listen(addr)
    client = await hat.event.server.eventer_client.create_eventer_client(
        addr=addr,
        client_name='client',
        local_server_id=1,
        remote_server_id=2,
        backend=backend)

    assert client.is_open

    await client.async_close()
    await server.async_close()
    await backend.async_close()


@pytest.mark.parametrize('local_server_id', [1, 42])
@pytest.mark.parametrize('event_count', [0, 1, 42])
async def test_synced(local_server_id, event_count, addr):
    remote_server_id = local_server_id + 1
    synced_future = asyncio.Future()

    def on_synced(srv_id, count):
        assert srv_id == remote_server_id
        assert count == event_count
        synced_future.set_result(None)

    def on_query(info, params):
        events = [common.Event(id=common.EventId(server=remote_server_id,
                                                 session=1,
                                                 instance=1 + i),
                               type=('x', str(i)),
                               timestamp=common.now(),
                               source_timestamp=None,
                               payload=None)
                  for i in range(event_count)]
        return common.QueryResult(events=events,
                                  more_follows=False)

    backend = Backend()
    server = await hat.event.eventer.server.listen(addr,
                                                   query_cb=on_query)
    client = await hat.event.server.eventer_client.create_eventer_client(
        addr=addr,
        client_name='client',
        local_server_id=local_server_id,
        remote_server_id=remote_server_id,
        backend=backend,
        synced_cb=on_synced)

    await synced_future

    await client.async_close()
    await server.async_close()
    await backend.async_close()


@pytest.mark.parametrize('local_server_id', [1, 42])
@pytest.mark.parametrize('event_count', [1, 42])
async def test_more_follows(local_server_id, event_count, addr):
    remote_server_id = local_server_id + 1
    counter = 0
    synced_future = asyncio.Future()

    def on_synced(srv_id, count):
        synced_future.set_result(None)

    def on_query(info, params):
        nonlocal counter
        counter += 1
        events = [common.Event(id=common.EventId(server=remote_server_id,
                                                 session=counter,
                                                 instance=1),
                               type=('x', ),
                               timestamp=common.now(),
                               source_timestamp=None,
                               payload=None)]
        return common.QueryResult(events=events,
                                  more_follows=counter < event_count)

    backend = Backend()
    server = await hat.event.eventer.server.listen(addr,
                                                   query_cb=on_query)
    client = await hat.event.server.eventer_client.create_eventer_client(
        addr=addr,
        client_name='client',
        local_server_id=local_server_id,
        remote_server_id=remote_server_id,
        backend=backend,
        synced_cb=on_synced)

    await synced_future

    assert counter == event_count

    await client.async_close()
    await server.async_close()
    await backend.async_close()


@pytest.mark.parametrize('local_server_id', [1, 42])
@pytest.mark.parametrize('event_count', [1, 42])
async def test_register(local_server_id, event_count, addr):
    remote_server_id = local_server_id + 1

    def on_query(info, params):
        assert isinstance(params, common.QueryServerParams)
        assert params.server_id == remote_server_id
        assert params.last_event_id == common.EventId(remote_server_id, 0, 0)

        events = [common.Event(id=common.EventId(server=remote_server_id,
                                                 session=i + 1,
                                                 instance=1),
                               type=('x', str(i)),
                               timestamp=common.now(),
                               source_timestamp=None,
                               payload=None)
                  for i in range(event_count)]
        return common.QueryResult(events=events,
                                  more_follows=False)

    register_queue = aio.Queue()
    backend = Backend(register_cb=register_queue.put_nowait)
    server = await hat.event.eventer.server.listen(addr,
                                                   query_cb=on_query)
    client = await hat.event.server.eventer_client.create_eventer_client(
        addr=addr,
        client_name='client',
        local_server_id=local_server_id,
        remote_server_id=remote_server_id,
        backend=backend)

    for i in range(event_count):
        events = await register_queue.get()
        assert len(events) == 1
        assert events[0].id == common.EventId(server=remote_server_id,
                                              session=i + 1,
                                              instance=1)

    assert register_queue.empty()
    assert client.synced

    await client.async_close()
    await server.async_close()
    await backend.async_close()


async def test_notify(addr):
    local_server_id = 1
    remote_server_id = 2

    events = [common.Event(id=common.EventId(server=remote_server_id,
                                             session=i + 1,
                                             instance=1),
                           type=('x', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(3)]

    async def on_query(info, params):
        await server.notify_events([events[1]], True)
        return common.QueryResult(events=[events[0]],
                                  more_follows=False)

    register_queue = aio.Queue()
    backend = Backend(register_cb=register_queue.put_nowait)
    server = await hat.event.eventer.server.listen(addr,
                                                   query_cb=on_query)
    client = await hat.event.server.eventer_client.create_eventer_client(
        addr=addr,
        client_name='client',
        local_server_id=local_server_id,
        remote_server_id=remote_server_id,
        backend=backend)

    result = await register_queue.get()
    assert list(result) == [events[0]]

    result = await register_queue.get()
    assert list(result) == [events[1]]

    assert register_queue.empty()
    assert client.synced

    await server.notify_events([events[2]], True)

    result = await register_queue.get()
    assert result == [events[2]]

    assert register_queue.empty()

    await client.async_close()
    await server.async_close()
    await backend.async_close()


async def test_remote_register_synced(addr):
    local_server_id = 1
    remote_server_id = 2
    event_queue = aio.Queue()
    query_queue = aio.Queue()

    def on_register(info, events):
        for event in events:
            event_queue.put_nowait(event)

    async def on_query(info, params):
        query_future = asyncio.Future()
        query_queue.put_nowait(query_future)
        await query_future
        return common.QueryResult(events=[],
                                  more_follows=False)

    backend = Backend()
    server = await hat.event.eventer.server.listen(addr,
                                                   register_cb=on_register,
                                                   query_cb=on_query)
    client = await hat.event.server.eventer_client.create_eventer_client(
        addr=addr,
        client_name='client',
        local_server_id=local_server_id,
        remote_server_id=remote_server_id,
        backend=backend)

    event = await event_queue.get()
    assert event.type == ('event', str(local_server_id), 'synced',
                          str(remote_server_id))
    assert event.payload.data is False

    query_future = await query_queue.get()
    assert event_queue.empty()
    query_future.set_result(None)

    event = await event_queue.get()
    assert event.type == ('event', str(local_server_id), 'synced',
                          str(remote_server_id))
    assert event.payload.data is True

    assert event_queue.empty()

    await client.async_close()
    await server.async_close()
    await backend.async_close()
