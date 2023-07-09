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


async def test_listen(address):
    srv_conn_queue = aio.Queue()
    server = await mariner.listen(address, srv_conn_queue.put_nowait)

    assert server.is_open

    await server.async_close()


async def test_connect(address):
    client_id = 'client id'
    client_token = 'client token'
    last_event_id = common.EventId(1, 2, 3)
    subscriptions = [('a', 'b', 'c')]

    srv_conn_queue = aio.Queue()
    server = await mariner.listen(address, srv_conn_queue.put_nowait)

    conn = await tcp.connect(address)
    conn = Transport(conn)

    assert srv_conn_queue.empty()

    await conn.send(common.InitMsg(client_id=client_id,
                                   client_token=client_token,
                                   last_event_id=last_event_id,
                                   subscriptions=subscriptions))
    await conn.drain()

    srv_conn = await srv_conn_queue.get()

    assert srv_conn.is_open
    assert srv_conn.client_id == client_id
    assert srv_conn.client_token == client_token
    assert srv_conn.last_event_id == last_event_id

    for i in subscriptions:
        assert srv_conn.subscription.matches(i)

    await server.async_close()
    assert not srv_conn.is_open

    await conn.async_close()


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

    srv_conn_queue = aio.Queue()
    server = await mariner.listen(address, srv_conn_queue.put_nowait)

    conn = await tcp.connect(address)
    conn = Transport(conn)

    assert srv_conn_queue.empty()

    await conn.send(common.InitMsg(client_id, None, None, []))
    await conn.drain()

    srv_conn = await srv_conn_queue.get()

    await srv_conn.send_events(events)

    msg = await conn.receive()
    assert msg == common.EventsMsg(events)

    await conn.async_close()
    await srv_conn.async_close()
    await server.async_close()


async def test_ping(address):
    client_id = 'client id'

    srv_conn_queue = aio.Queue()
    server = await mariner.listen(address, srv_conn_queue.put_nowait,
                                  ping_delay=0.01)

    conn = await tcp.connect(address)
    conn = Transport(conn)

    assert srv_conn_queue.empty()

    await conn.send(common.InitMsg(client_id, None, None, []))
    await conn.drain()

    srv_conn = await srv_conn_queue.get()

    for i in range(10):
        msg = await conn.receive()
        assert msg == common.PingMsg()

        await conn.send(common.PongMsg())
        await conn.drain()

    await conn.async_close()
    await srv_conn.async_close()
    await server.async_close()


async def test_pong(address):
    client_id = 'client id'

    srv_conn_queue = aio.Queue()
    server = await mariner.listen(address, srv_conn_queue.put_nowait)

    conn = await tcp.connect(address)
    conn = Transport(conn)

    assert srv_conn_queue.empty()

    await conn.send(common.InitMsg(client_id, None, None, []))
    await conn.drain()

    srv_conn = await srv_conn_queue.get()

    await conn.send(common.PingMsg())
    await conn.drain()

    msg = await conn.receive()
    assert msg == common.PongMsg()

    await conn.async_close()
    await srv_conn.async_close()
    await server.async_close()
