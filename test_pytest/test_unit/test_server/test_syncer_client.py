import asyncio

import pytest

from hat import aio
from hat import chatter
from hat import util
import hat.monitor.common

from hat.event.server import common
from hat.event.server.syncer_client import create_syncer_client, SyncerClient


@pytest.fixture
def server_address_factory():

    def f():
        server_port = util.get_unused_tcp_port()
        return f'tcp+sbs://127.0.0.1:{server_port}'

    return f


async def create_server(address):
    server = Server()
    server._conn_queue = aio.Queue()
    server._srv = await chatter.listen(
        common.sbs_repo, address,
        lambda conn: server._conn_queue.put_nowait(conn))
    return server


class Server(aio.Resource):

    @property
    def async_group(self):
        return self._srv.async_group

    async def get_connection(self):
        return await self._conn_queue.get()


class Backend(aio.Resource):

    def __init__(self, last_session_id=0, last_instance_id=0):
        self._async_group = aio.Group()
        self._flushed_events_cbs = util.CallbackRegistry()
        self._last_session_id = last_session_id
        self._last_instance_id = last_instance_id
        self._from_event_id = None

    @property
    def async_group(self):
        return self._async_group

    async def register(self, events):
        self._flushed_events_cbs.notify(events)
        return events

    def register_flushed_events_cb(self, cb):
        return self._flushed_events_cbs.register(cb)

    async def flush(self):
        pass

    async def get_last_event_id(self, server_id):
        return common.EventId(server=server_id,
                              session=self._last_session_id,
                              instance=self._last_instance_id)


class MonitorClient(aio.Resource):

    def __init__(self):
        self._async_group = aio.Group()
        self._components = []
        self._change_cbs = util.CallbackRegistry()

    @property
    def async_group(self):
        return self._async_group

    @property
    def components(self):
        return self._components

    @property
    def info(self):
        return None

    def register_change_cb(self, cb):
        return self._change_cbs.register(cb)

    def change(self, components):
        self._components = components
        self._change_cbs.notify()


async def test_create():
    backend = Backend()
    monitor_client = MonitorClient()
    syncer_client = await create_syncer_client(
        backend, monitor_client, 'group', 'name')

    assert isinstance(syncer_client, SyncerClient)
    assert syncer_client.is_open

    syncer_client.close()
    await syncer_client.wait_closing()
    assert not syncer_client.is_open
    assert syncer_client.is_closing
    await syncer_client.wait_closed()
    assert syncer_client.is_closed


@pytest.mark.parametrize("syncer_token",  ['abc123', None])
async def test_connect(server_address_factory, syncer_token):
    server_address = server_address_factory()
    server_id = 13
    last_session_id = 789
    last_instance_id = 112233
    group = 'abc'
    client_name = 'c1'
    events_queue = aio.Queue()
    backend = Backend(last_session_id, last_instance_id)
    backend.register_flushed_events_cb(
        lambda evts: events_queue.put_nowait(evts))
    monitor_client = MonitorClient()

    if syncer_token is not None:
        syncer_client = await create_syncer_client(
            backend, monitor_client, group, client_name, syncer_token)
    else:
        syncer_client = await create_syncer_client(
            backend, monitor_client, group, client_name)

    syncer_server = await create_server(server_address)
    component_data = {'syncer_server_address': server_address,
                      'server_id': server_id}
    if syncer_token is not None:
        component_data['syncer_token'] = syncer_token
    monitor_client.change(components=[
        hat.monitor.common.ComponentInfo(
            cid=1,
            mid=2,
            name='name',
            group=group,
            data=component_data,
            rank=3,
            blessing_req=hat.monitor.common.BlessingReq(token=123,
                                                        timestamp=456),
            blessing_res=hat.monitor.common.BlessingRes(token=123,
                                                        ready=True))])

    conn = await syncer_server.get_connection()
    msg = await conn.receive()
    assert msg.data.type == 'MsgReq'
    assert msg.data.data['lastEventId']['server'] == server_id
    assert msg.data.data['lastEventId']['session'] == last_session_id
    assert msg.data.data['lastEventId']['instance'] == last_instance_id
    assert msg.data.data['clientName'] == client_name

    events = [common.Event(
        event_id=common.EventId(server=server_id,
                                session=last_session_id + 1,
                                instance=last_instance_id + (i + 1)),
        event_type=('a', 'b', 'cdef', str(i)),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayload(common.EventPayloadType.JSON, i))
        for i in range(10)]
    conn.send(chatter.Data(module='HatSyncer',
                           type='MsgEvents',
                           data=[common.event_to_sbs(e) for e in events]))
    events_registered = await events_queue.get()
    assert events == events_registered

    await syncer_client.async_close()
    await syncer_server.async_close()
    await backend.async_close()
    await monitor_client.async_close()


async def test_token_not_valid(server_address_factory):
    server_address = server_address_factory()
    group = 'abc'
    syncer_token = 'abc123'
    client_name = 'c1'
    events_queue = aio.Queue()
    backend = Backend()
    backend.register_flushed_events_cb(
        lambda evts: events_queue.put_nowait(evts))
    monitor_client = MonitorClient()
    syncer_client = await create_syncer_client(backend, monitor_client, group,
                                               client_name, syncer_token)

    syncer_server = await create_server(server_address)

    monitor_client.change(components=[
        hat.monitor.common.ComponentInfo(
            cid=1,
            mid=2,
            name='name',
            group=group,
            data={'syncer_server_address': server_address,
                  'server_id': 13,
                  'syncer_token': 'blabla'},
            rank=3,
            blessing_req=hat.monitor.common.BlessingReq(token=123,
                                                        timestamp=456),
            blessing_res=hat.monitor.common.BlessingRes(token=123,
                                                        ready=True))])
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(syncer_server.get_connection(), 0.01)

    await syncer_client.async_close()
    await syncer_server.async_close()
    await backend.async_close()
    await monitor_client.async_close()


async def test_connect_multiple(server_address_factory):
    servers_cnt = 10
    group = 'abc'
    syncer_token = 'abc123'
    client_name = 'c1'
    events_queue = aio.Queue()
    backend = Backend()
    backend.register_flushed_events_cb(
        lambda evts: events_queue.put_nowait(evts))
    monitor_client = MonitorClient()
    syncer_client = await create_syncer_client(backend, monitor_client, group,
                                               client_name, syncer_token)

    servers = []
    for i in range(servers_cnt):
        srv_id = 123 + i * 2
        server_address = server_address_factory()
        srv = await create_server(server_address)
        servers.append((server_address, srv_id, srv))
    monitor_client.change(components=[
        hat.monitor.common.ComponentInfo(
            cid=srv_id,
            mid=2,
            name='name',
            group=group,
            data={'syncer_server_address': server_address,
                  'server_id': srv_id,
                  'syncer_token': syncer_token},
            rank=3,
            blessing_req=hat.monitor.common.BlessingReq(token=123,
                                                        timestamp=456),
            blessing_res=hat.monitor.common.BlessingRes(token=123,
                                                        ready=True))
        for server_address, srv_id, _ in servers])

    for _, srv_id, srv in servers:
        conn = await srv.get_connection()
        msg = await conn.receive()
        assert msg.data.type == 'MsgReq'
        assert msg.data.data['lastEventId']['server'] == srv_id

    for _, _, srv in servers:
        await srv.async_close()

    await syncer_client.async_close()
    await backend.async_close()
    await monitor_client.async_close()
