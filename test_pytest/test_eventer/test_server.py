import asyncio

import pytest

from hat import aio
from hat import util
from hat.drivers import tcp
from hat.drivers import chatter

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
async def test_init(addr, client_name, client_token, subscriptions,
                    server_id, persisted):
    connected_queue = aio.Queue()
    disconnected_queue = aio.Queue()

    srv = await hat.event.eventer.listen(
        addr,
        connected_cb=connected_queue.put_nowait,
        disconnected_cb=disconnected_queue.put_nowait)

    conn = await chatter.connect(addr)

    assert conn.is_open
    assert connected_queue.empty()
    assert disconnected_queue.empty()

    await common.send_msg(conn, 'HatEventer.MsgInitReq',
                          {'clientName': client_name,
                           'clientToken': optional_to_sbs(client_token),
                           'subscriptions': [list(i) for i in subscriptions],
                           'serverId': optional_to_sbs(server_id),
                           'persisted': persisted},
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgInitRes'
    assert msg_data == ('success', ('standby', None))

    info = await connected_queue.get()
    assert info.client_name == client_name
    assert info.client_token == client_token
    assert set(info.subscription.get_query_types()) == set(subscriptions)
    assert info.server_id == server_id
    assert info.persisted == persisted

    assert conn.is_open
    assert disconnected_queue.empty()

    await conn.async_close()

    result = await disconnected_queue.get()
    assert info == result

    await srv.async_close()


async def test_init_error(addr):

    def on_connected(info):
        raise Exception('abc')

    srv = await hat.event.eventer.listen(addr,
                                         connected_cb=on_connected)
    conn = await chatter.connect(addr)

    await common.send_msg(conn, 'HatEventer.MsgInitReq',
                          {'clientName': 'name',
                           'clientToken': optional_to_sbs(None),
                           'subscriptions': [],
                           'serverId': optional_to_sbs(None),
                           'persisted': False},
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgInitRes'
    assert msg_data == ('error', 'abc')

    await conn.async_close()
    await srv.async_close()


async def test_status(addr):
    srv = await hat.event.eventer.listen(addr)
    conn = await chatter.connect(addr)

    await common.send_msg(conn, 'HatEventer.MsgInitReq',
                          {'clientName': 'name',
                           'clientToken': optional_to_sbs(None),
                           'subscriptions': [],
                           'serverId': optional_to_sbs(None),
                           'persisted': False},
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgInitRes'
    assert msg_data == ('success', ('standby', None))

    status_future = asyncio.create_task(
        srv.set_status(common.Status.OPERATIONAL))

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgStatusNotify'
    assert msg_data == ('operational', None)

    assert not status_future.done()

    await common.send_msg(conn, 'HatEventer.MsgStatusAck', None,
                          conv=msg.conv)

    await status_future

    await conn.async_close()
    await srv.async_close()


@pytest.mark.parametrize('server_id', [None, 123])
@pytest.mark.parametrize('persisted', [True, False])
async def test_events(addr, server_id, persisted):
    srv = await hat.event.eventer.listen(addr)
    conn = await chatter.connect(addr)

    await common.send_msg(conn, 'HatEventer.MsgInitReq',
                          {'clientName': 'name',
                           'clientToken': optional_to_sbs(None),
                           'subscriptions': [['a']],
                           'serverId': optional_to_sbs(server_id),
                           'persisted': persisted},
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgInitRes'
    assert msg_data == ('success', ('standby', None))

    events = [common.Event(id=common.EventId((server_id or 1), 1, 1),
                           type=('a',),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None),
              common.Event(id=common.EventId((server_id or 1) + 1, 1, 1),
                           type=('a',),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None),
              common.Event(id=common.EventId((server_id or 1), 1, 2),
                           type=('b',),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)]

    await srv.notify_events(events, persisted)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgEventsNotify'
    assert [common.event_from_sbs(i) for i in msg_data] == (
        [events[0], events[1]] if server_id is None else [events[0]])

    await srv.notify_events(events, not persisted)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(common.receive_msg(conn), 0.01)

    await conn.async_close()
    await srv.async_close()


@pytest.mark.parametrize('register_events', [
    [],
    [common.RegisterEvent(type=('a', 'b', 'c'),
                          source_timestamp=common.now(),
                          payload=common.EventPayloadJson(i))
     for i in range(3)]
])
@pytest.mark.parametrize('success', [True, False])
async def test_register(addr, register_events, success):
    events = [common.Event(id=common.EventId(1, 1, 1 + i),
                           type=event.type,
                           timestamp=common.now(),
                           source_timestamp=event.source_timestamp,
                           payload=event.payload)
              for i, event in enumerate(register_events)]

    def on_register(info, evs):
        assert evs == register_events
        return events if success else None

    srv = await hat.event.eventer.listen(addr,
                                         register_cb=on_register)
    conn = await chatter.connect(addr)

    await common.send_msg(conn, 'HatEventer.MsgInitReq',
                          {'clientName': 'name',
                           'clientToken': optional_to_sbs(None),
                           'subscriptions': [],
                           'serverId': optional_to_sbs(None),
                           'persisted': None},
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgInitRes'
    assert msg_data == ('success', ('standby', None))

    await common.send_msg(conn, 'HatEventer.MsgRegisterReq',
                          [common.register_event_to_sbs(i)
                           for i in register_events],
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgRegisterRes'
    if success:
        assert msg_data == ('events', [common.event_to_sbs(i) for i in events])
    else:
        assert msg_data == ('failure', None)

    await conn.async_close()
    await srv.async_close()


@pytest.mark.parametrize('params', [common.QueryLatestParams(),
                                    common.QueryTimeseriesParams(),
                                    common.QueryServerParams(123)])
@pytest.mark.parametrize('result', [common.QueryResult(events=[],
                                                       more_follows=False)])
async def test_query(addr, params, result):

    def on_query(info, p):
        assert p == params
        return result

    srv = await hat.event.eventer.listen(addr,
                                         query_cb=on_query)
    conn = await chatter.connect(addr)

    await common.send_msg(conn, 'HatEventer.MsgInitReq',
                          {'clientName': 'name',
                           'clientToken': optional_to_sbs(None),
                           'subscriptions': [],
                           'serverId': optional_to_sbs(None),
                           'persisted': None},
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgInitRes'
    assert msg_data == ('success', ('standby', None))

    await common.send_msg(conn, 'HatEventer.MsgQueryReq',
                          common.query_params_to_sbs(params),
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatEventer.MsgQueryRes'
    assert msg_data == common.query_result_to_sbs(result)

    await conn.async_close()
    await srv.async_close()
