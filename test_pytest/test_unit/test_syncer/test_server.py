import pytest

from hat import aio
from hat import util

from hat.event import common
import hat.event.syncer


@pytest.fixture
def port():
    return util.get_unused_tcp_port()


@pytest.fixture
def address(port):
    return f'tcp+sbs://127.0.0.1:{port}'


async def test_create(address):
    server = await hat.event.syncer.listen(address)

    assert server.is_open

    await server.async_close()


async def test_connect(address):
    server = await hat.event.syncer.listen(address)

    state_queue = aio.Queue()
    server.register_state_cb(state_queue.put_nowait)

    assert state_queue.empty()
    assert server.state == []

    client = await hat.event.syncer.connect(
        address=address,
        client_name='name123',
        last_event_id=common.EventId(1, 2, 3))

    status = await state_queue.get()
    assert status == [hat.event.syncer.ClientInfo(name='name123',
                                                  synced=False)]

    status = await state_queue.get()
    assert status == [hat.event.syncer.ClientInfo(name='name123',
                                                  synced=True)]

    await client.async_close()

    status = await state_queue.get()
    assert status == []

    await server.async_close()


async def test_query(address):
    query_queue = aio.Queue()
    events = [common.Event(event_id=common.EventId(1, i + 1, 1),
                           event_type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(10)]

    async def on_query(last_event_id):
        query_queue.put_nowait(last_event_id)
        for event in events:
            yield [event]

    server = await hat.event.syncer.listen(address, on_query)

    events_queue = aio.Queue()
    client = await hat.event.syncer.connect(
        address=address,
        client_name='name123',
        last_event_id=common.EventId(1, 0, 0),
        events_cb=events_queue.put_nowait)

    last_event_id = await query_queue.get()
    assert last_event_id == common.EventId(1, 0, 0)

    for event in events:
        result = await events_queue.get()
        assert result == [event]

    await client.async_close()
    await server.async_close()


async def test_notify(address):
    events = [common.Event(event_id=common.EventId(1, i + 1, 1),
                           event_type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(10)]

    server = await hat.event.syncer.listen(address)

    synced_queue = aio.Queue()
    events_queue = aio.Queue()
    client = await hat.event.syncer.connect(
        address=address,
        client_name='name123',
        last_event_id=common.EventId(1, 0, 0),
        synced_cb=lambda: synced_queue.put_nowait(None),
        events_cb=events_queue.put_nowait)

    await synced_queue.get()

    server.notify(events)

    result = await events_queue.get()
    assert result == events

    await client.async_close()
    await server.async_close()


async def test_flush(address):
    server = await hat.event.syncer.listen(address)

    synced_queue = aio.Queue()
    client = await hat.event.syncer.connect(
        address=address,
        client_name='name123',
        last_event_id=common.EventId(1, 0, 0),
        synced_cb=lambda: synced_queue.put_nowait(None))

    await synced_queue.get()

    await server.flush()

    await client.async_close()

    await server.flush()

    await server.async_close()

    await server.flush()
