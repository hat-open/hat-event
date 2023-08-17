import asyncio

import pytest

from hat import aio
from hat import util
from hat.drivers import chatter
from hat.drivers import tcp

from hat.event.eventer import common
import hat.event.eventer


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


def optional_to_sbs(value):
    return ('value', value) if value is not None else ('none', None)


@pytest.mark.parametrize('client_name', ['name123'])
@pytest.mark.parametrize('client_token', [None, 'token'])
@pytest.mark.parametrize('subscriptions', [[],
                                           [('*',)],
                                           [('a',), ('b',), ('a', 'b', 'c')]])
@pytest.mark.parametrize('server_id', [None, 123])
@pytest.mark.parametrize('persisted', [True, False])
@pytest.mark.parametrize('error', [None, 'error123'])
async def test_connect(addr, client_name, client_token, subscriptions,
                       server_id, persisted, error):
    with pytest.raises(ConnectionError):
        await hat.event.eventer.connect(addr, client_name)

    conn_queue = aio.Queue()
    srv = await chatter.listen(conn_queue.put_nowait, addr)

    client_task = asyncio.create_task(
        hat.event.eventer.connect(addr, client_name,
                                  client_token=client_token,
                                  subscriptions=subscriptions,
                                  server_id=server_id,
                                  persisted=persisted))

    conn = await conn_queue.get()
    msg, msg_type, msg_data = await common.receive_msg(conn)

    assert msg_type == 'HatEventer.MsgInitReq'
    assert msg_data == {'clientName': client_name,
                        'clientToken': optional_to_sbs(client_token),
                        'subscriptions': [list(i) for i in subscriptions],
                        'serverId': optional_to_sbs(server_id),
                        'persisted': persisted}

    res_data = ('error', error) if error else ('success', ('standby', None))
    await common.send_msg(conn, 'HatEventer.MsgInitRes', res_data,
                          conv=msg.conv)

    if error:
        with pytest.raises(hat.event.eventer.EventerInitError):
            client = await client_task

    else:
        client = await client_task

        assert conn.is_open
        assert client.is_open

        await client.async_close()

    await srv.async_close()


async def test_status(addr):
    status_queue = aio.Queue()

    def on_status(client, status):
        status_queue.put_nowait(status)

    conn_queue = aio.Queue()
    srv = await chatter.listen(conn_queue.put_nowait, addr)

    client_task = asyncio.create_task(
        hat.event.eventer.connect(addr, 'name',
                                  status_cb=on_status))

    conn = await conn_queue.get()
    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgInitReq'

    await common.send_msg(conn, 'HatEventer.MsgInitRes',
                          ('success', ('standby', None)),
                          conv=msg.conv)

    client = await client_task

    assert client.status == common.Status.STANDBY
    assert status_queue.empty()

    await common.send_msg(conn, 'HatEventer.MsgStatusNotify',
                          ('operational', None),
                          last=False)

    status = await status_queue.get()
    assert status == common.Status.OPERATIONAL
    assert status_queue.empty()

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgStatusAck'
    assert msg_data is None

    await client.async_close()
    await srv.async_close()


@pytest.mark.parametrize('register_events', [
    [],
    [common.RegisterEvent(type=('a', 'b', 'c'),
                          source_timestamp=common.now(),
                          payload=common.EventPayloadJson(i))
     for i in range(3)]
])
@pytest.mark.parametrize('with_response', [True, False])
@pytest.mark.parametrize('failure', [True, False])
async def test_register(addr, register_events, with_response, failure):
    conn_queue = aio.Queue()
    srv = await chatter.listen(conn_queue.put_nowait, addr)

    client_task = asyncio.create_task(hat.event.eventer.connect(addr, 'name'))

    conn = await conn_queue.get()
    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgInitReq'

    await common.send_msg(conn, 'HatEventer.MsgInitRes',
                          ('success', ('standby', None)),
                          conv=msg.conv)

    client = await client_task

    register_task = asyncio.create_task(
        client.register(register_events, with_response))

    if not with_response:
        result = await register_task
        assert result is None

    else:
        msg, msg_type, msg_data = await common.receive_msg(conn)
        assert msg_type == 'HatEventer.MsgRegisterReq'
        assert msg_data == [common.register_event_to_sbs(i)
                            for i in register_events]

        events = [common.Event(id=common.EventId(1, 1, 1 + i),
                               type=event.type,
                               timestamp=common.now(),
                               source_timestamp=event.source_timestamp,
                               payload=event.payload)
                  for i, event in enumerate(register_events)]

        res_data = (('failure', None) if failure else
                    ('events', [common.event_to_sbs(i) for i in events]))
        await common.send_msg(conn, 'HatEventer.MsgRegisterRes', res_data,
                              conv=msg.conv)

        result = await register_task
        if failure:
            assert result is None
        else:
            assert result == events

    await client.async_close()
    await srv.async_close()


@pytest.mark.parametrize('params', [common.QueryLatestParams(),
                                    common.QueryTimeseriesParams(),
                                    common.QueryServerParams(123)])
@pytest.mark.parametrize('result', [common.QueryResult(events=[],
                                                       more_follows=False)])
async def test_query(addr, params, result):
    conn_queue = aio.Queue()
    srv = await chatter.listen(conn_queue.put_nowait, addr)

    client_task = asyncio.create_task(hat.event.eventer.connect(addr, 'name'))

    conn = await conn_queue.get()
    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgInitReq'

    await common.send_msg(conn, 'HatEventer.MsgInitRes',
                          ('success', ('standby', None)),
                          conv=msg.conv)

    client = await client_task

    query_task = asyncio.create_task(client.query(params))
    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgQueryReq'
    assert msg_data == common.query_params_to_sbs(params)

    await common.send_msg(conn, 'HatEventer.MsgQueryRes',
                          common.query_result_to_sbs(result),
                          conv=msg.conv)

    query_result = await query_task
    assert query_result == result

    await client.async_close()
    await srv.async_close()


@pytest.mark.parametrize('events', [
    [common.Event(id=common.EventId(1, 1, 1 + i),
                  type=('a', 'b', 'c'),
                  timestamp=common.now(),
                  source_timestamp=common.now(),
                  payload=common.EventPayloadJson(i))
     for i in range(3)]
])
async def test_events(addr, events):
    events_queue = aio.Queue()

    def on_events(client, events):
        events_queue.put_nowait(events)

    conn_queue = aio.Queue()
    srv = await chatter.listen(conn_queue.put_nowait, addr)

    client_task = asyncio.create_task(
        hat.event.eventer.connect(addr, 'name',
                                  events_cb=on_events))

    conn = await conn_queue.get()
    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgInitReq'

    await common.send_msg(conn, 'HatEventer.MsgInitRes',
                          ('success', ('standby', None)),
                          conv=msg.conv)

    client = await client_task

    await common.send_msg(conn, 'HatEventer.MsgEventsNotify',
                          [common.event_to_sbs(event) for event in events])

    result = await events_queue.get()
    assert result == events

    await client.async_close()
    await srv.async_close()
