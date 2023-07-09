import pytest

from hat import aio
from hat import util
from hat.drivers import tcp

from hat.event import mariner
from hat.event.mariner import common
from hat.event.mariner.transport import Transport


@pytest.fixture
def port():
    return util.get_unused_tcp_port()


@pytest.fixture
def address(port):
    return tcp.Address('127.0.0.1', port)


async def test_connect(address):
    client_id = 'client id'
    client_token = 'client token'
    last_event_id = common.EventId(1, 2, 3)
    subscriptions = [('a', 'b', 'c')]

    conn_queue = aio.Queue()
    server = await tcp.listen(conn_queue.put_nowait, address)

    client = await mariner.connect(address=address,
                                   client_id=client_id,
                                   client_token=client_token,
                                   last_event_id=last_event_id,
                                   subscriptions=subscriptions)

    conn = await conn_queue.get()
    conn = Transport(conn)

    assert conn.is_open

    msg = await conn.receive()
    assert msg == common.InitMsg(client_id=client_id,
                                 client_token=client_token,
                                 last_event_id=last_event_id,
                                 subscriptions=subscriptions)

    await conn.async_close()
    await client.wait_closed()

    await server.async_close()


@pytest.mark.parametrize('events', [
    [],
    [common.Event(event_id=common.EventId(1, 1, i + 1),
                  event_type=('a', 'b', 'c', str(i)),
                  timestamp=common.now(),
                  source_timestamp=None,
                  payload=None)
     for i in range(10)]])
async def test_events(address, events):
    client_id = 'client id'

    conn_queue = aio.Queue()
    server = await tcp.listen(conn_queue.put_nowait, address)

    events_queue = aio.Queue()
    client = await mariner.connect(address=address,
                                   client_id=client_id,
                                   events_cb=events_queue.put_nowait)

    conn = await conn_queue.get()
    conn = Transport(conn)

    msg = await conn.receive()
    assert msg == common.InitMsg(client_id, None, None, [])

    await conn.send(common.EventsMsg(events))
    await conn.drain()

    result = await events_queue.get()
    assert result == events

    await conn.async_close()
    await client.wait_closed()

    await server.async_close()


async def test_ping(address):
    client_id = 'client id'

    conn_queue = aio.Queue()
    server = await tcp.listen(conn_queue.put_nowait, address)

    client = await mariner.connect(address=address,
                                   client_id=client_id,
                                   ping_delay=0.01)

    conn = await conn_queue.get()
    conn = Transport(conn)

    msg = await conn.receive()
    assert msg == common.InitMsg(client_id, None, None, [])

    for i in range(10):
        msg = await conn.receive()
        assert msg == common.PingMsg()

        await conn.send(common.PongMsg())
        await conn.drain()

    await conn.async_close()
    await client.wait_closed()

    await server.async_close()


async def test_pong(address):
    client_id = 'client id'

    conn_queue = aio.Queue()
    server = await tcp.listen(conn_queue.put_nowait, address)

    client = await mariner.connect(address=address,
                                   client_id=client_id)

    conn = await conn_queue.get()
    conn = Transport(conn)

    msg = await conn.receive()
    assert msg == common.InitMsg(client_id, None, None, [])

    await conn.send(common.PingMsg())
    await conn.drain()

    msg = await conn.receive()
    assert msg == common.PongMsg()

    await conn.async_close()
    await client.wait_closed()

    await server.async_close()
