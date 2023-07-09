import pytest

from hat import aio
from hat import util
from hat.drivers import tcp

from hat.event.mariner import common
from hat.event.mariner.transport import Transport


@pytest.fixture
def port():
    return util.get_unused_tcp_port()


@pytest.fixture
def address(port):
    return tcp.Address('127.0.0.1', port)


@pytest.mark.parametrize('msg', [
    common.PingMsg(),
    common.PongMsg(),
    common.InitMsg(client_id='client id',
                   client_token=None,
                   last_event_id=None,
                   subscriptions=[]),
    common.EventsMsg(events=[])])
async def test_send_receive(address, msg):
    conn_queue = aio.Queue()
    server = await tcp.listen(conn_queue.put_nowait, address)
    conn1 = await tcp.connect(address)
    conn2 = await conn_queue.get()
    await server.async_close()

    conn1 = Transport(conn1)
    conn2 = Transport(conn2)

    await conn1.send(msg)
    await conn1.drain()

    received = await conn2.receive()

    assert received == msg

    await conn1.async_close()
    await conn2.async_close()
