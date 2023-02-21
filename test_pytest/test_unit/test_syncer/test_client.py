import pytest

from hat import aio
from hat import chatter
from hat import util

from hat.event import common
import hat.event.syncer


@pytest.fixture
def port():
    return util.get_unused_tcp_port()


@pytest.fixture
def address(port):
    return f'tcp+sbs://127.0.0.1:{port}'


@pytest.fixture
async def conn_queue(address):
    conn_queue = aio.Queue()
    server = await chatter.listen(common.sbs_repo, address,
                                  conn_queue.put_nowait)

    try:
        yield conn_queue

    finally:
        await server.async_close()


async def test_connect(address, conn_queue):
    client = await hat.event.syncer.connect(
        address=address,
        client_name='name123',
        last_event_id=common.EventId(1, 2, 3))

    conn = await conn_queue.get()

    assert client.is_open
    assert conn.is_open

    await client.async_close()
    await conn.async_close()


async def test_sync_req(address, conn_queue):
    client = await hat.event.syncer.connect(
        address=address,
        client_name='name123',
        last_event_id=common.EventId(1, 2, 3))
    conn = await conn_queue.get()

    msg = await conn.receive()
    assert msg.first is True
    assert msg.last is True
    assert msg.data.module == 'HatSyncer'
    assert msg.data.type == 'MsgReq'
    assert msg.data.data == {'lastEventId': {'server': 1,
                                             'session': 2,
                                             'instance': 3},
                             'clientName': 'name123'}

    await client.async_close()
    await conn.async_close()


async def test_flush(address, conn_queue):
    client = await hat.event.syncer.connect(
        address=address,
        client_name='name123',
        last_event_id=common.EventId(1, 2, 3))
    conn = await conn_queue.get()

    msg = await conn.receive()
    assert msg.data.module == 'HatSyncer'
    assert msg.data.type == 'MsgReq'

    conv = conn.send(chatter.Data('HatSyncer', 'MsgFlushReq', None),
                     last=False)

    msg = await conn.receive()
    assert msg.conv == conv
    assert msg.first is False
    assert msg.last is True
    assert msg.data.module == 'HatSyncer'
    assert msg.data.type == 'MsgFlushRes'
    assert msg.data.data is None

    await client.async_close()
    await conn.async_close()


async def test_synced(address, conn_queue):
    synced_queue = aio.Queue()
    client = await hat.event.syncer.connect(
        address=address,
        client_name='name123',
        last_event_id=common.EventId(1, 2, 3),
        synced_cb=lambda: synced_queue.put_nowait(None))
    conn = await conn_queue.get()

    msg = await conn.receive()
    assert msg.data.module == 'HatSyncer'
    assert msg.data.type == 'MsgReq'

    assert synced_queue.empty()

    conn.send(chatter.Data('HatSyncer', 'MsgSynced', None))

    await synced_queue.get()

    await client.async_close()
    await conn.async_close()


async def test_events(address, conn_queue):
    events = [common.Event(event_id=common.EventId(1, 1, i + 1),
                           event_type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(10)]

    events_queue = aio.Queue()
    client = await hat.event.syncer.connect(
        address=address,
        client_name='name123',
        last_event_id=common.EventId(1, 2, 3),
        events_cb=events_queue.put_nowait)
    conn = await conn_queue.get()

    msg = await conn.receive()
    assert msg.data.module == 'HatSyncer'
    assert msg.data.type == 'MsgReq'

    assert events_queue.empty()

    conn.send(chatter.Data('HatSyncer', 'MsgEvents',
                           [common.event_to_sbs(event) for event in events]))

    result = await events_queue.get()
    assert events == result

    await client.async_close()
    await conn.async_close()
