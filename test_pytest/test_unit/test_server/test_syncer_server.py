import asyncio

import pytest

from hat import aio
from hat import chatter
from hat import util

from hat.event.server import common
from hat.event.server.syncer_server import create_syncer_server, SyncerServer


@pytest.fixture
def server_port():
    return util.get_unused_tcp_port()


@pytest.fixture
def server_address(server_port):
    return f'tcp+sbs://127.0.0.1:{server_port}'


@pytest.fixture
def conf(server_address):
    return {'address': server_address}


def create_backend(query_from_event_id_events=[]):
    backend = Backend()
    backend._async_group = aio.Group()
    backend._flushed_events_cbs = util.CallbackRegistry()
    backend._events = list(query_from_event_id_events)
    backend._from_event_id = None
    return backend


class Backend(aio.Resource):

    @property
    def async_group(self):
        return self._async_group

    @property
    def from_event_id(self):
        return self._from_event_id

    def register_flushed_events_cb(self, cb):
        return self._flushed_events_cbs.register(cb)

    async def register(self, events):
        self._events.extend(events)
        self._flushed_events_cbs.notify(events)
        return events

    async def query_flushed(self, event_id):
        self._from_event_id = event_id
        if len(self._events) != 0:
            yield self._events

    async def flush(self):
        pass


async def test_create(conf):
    backend = create_backend()
    syncer_server = await create_syncer_server(conf, backend)

    assert isinstance(syncer_server, SyncerServer)
    assert syncer_server.is_open

    syncer_server.close()
    await syncer_server.wait_closing()
    assert syncer_server.is_closing
    await syncer_server.wait_closed()
    assert syncer_server.is_closed


async def test_connect(conf):
    state_queue = aio.Queue()
    backend = create_backend()
    syncer_server = await create_syncer_server(conf, backend)
    syncer_server.register_state_cb(state_queue.put_nowait)
    client_name = 'abcd1234'

    conn = await chatter.connect(common.sbs_repo, conf['address'])
    assert conn.is_open
    assert state_queue.empty()

    conn.send(chatter.Data(module='HatSyncer',
                           type='MsgReq',
                           data={'lastEventId': {'server': 1,
                                                 'session': 0,
                                                 'instance': 0},
                                 'clientName': client_name}))
    state = await state_queue.get()
    assert len(state) == 1
    assert state[0].name == client_name
    assert not state[0].synced

    msg = await conn.receive()
    assert msg.data.type == 'MsgSynced'
    assert msg.data.data is None

    state = await state_queue.get()
    assert len(state) == 1
    assert state[0].name == client_name
    assert state[0].synced

    conn.close()

    state = await state_queue.get()
    assert len(state) == 0

    await conn.wait_closed()
    await backend.async_close()
    await syncer_server.async_close()


async def test_sync(conf):
    events = [common.Event(
        event_id=common.EventId(server=1,
                                session=2,
                                instance=i),
        event_type=('a', 'b', 'c'),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayload(common.EventPayloadType.JSON, i))
        for i in range(10)]
    state_queue = aio.Queue()
    backend = create_backend(query_from_event_id_events=events)
    syncer_server = await create_syncer_server(conf, backend)
    syncer_server.register_state_cb(state_queue.put_nowait)

    conn = await chatter.connect(common.sbs_repo, conf['address'])
    assert conn.is_open
    assert state_queue.empty()

    last_event_id = common.EventId(server=1, session=1, instance=456)
    conn.send(chatter.Data(module='HatSyncer',
                           type='MsgReq',
                           data={'lastEventId': last_event_id._asdict(),
                                 'clientName': 'abcd'}))
    state = await state_queue.get()
    assert len(state) == 1
    assert not state[0].synced

    msg = await conn.receive()
    assert msg.data.type == 'MsgEvents'
    assert [common.event_from_sbs(i) for i in msg.data.data] == events
    assert backend.from_event_id == last_event_id

    msg = await conn.receive()
    assert msg.data.type == 'MsgSynced'
    assert msg.data.data is None
    state = await state_queue.get()
    assert len(state) == 1
    assert state[0].synced

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.01)

    await conn.async_close()
    await backend.async_close()
    await syncer_server.async_close()


async def test_register(conf):
    state_queue = aio.Queue()
    backend = create_backend()
    syncer_server = await create_syncer_server(conf, backend)
    syncer_server.register_state_cb(state_queue.put_nowait)

    conn = await chatter.connect(common.sbs_repo, conf['address'])
    assert conn.is_open
    assert state_queue.empty()

    conn.send(chatter.Data(module='HatSyncer',
                           type='MsgReq',
                           data={'lastEventId': {'server': 1,
                                                 'session': 0,
                                                 'instance': 0},
                                 'clientName': 'abcd'}))

    msg = await conn.receive()
    assert msg.data.type == 'MsgSynced'
    state = await state_queue.get()
    assert len(state) == 1
    assert not state[0].synced

    for session, events_length in enumerate([2, 4, 13]):
        events = [common.Event(
            event_id=common.EventId(server=1,
                                    session=session + 1,
                                    instance=i),
            event_type=('a', 'b', 'c'),
            timestamp=common.now(),
            source_timestamp=common.now(),
            payload=common.EventPayload(common.EventPayloadType.JSON, i))
            for i in range(events_length)]
        await backend.register(events)
        msg = await conn.receive()
        assert msg.data.type == 'MsgEvents'
        assert len(msg.data.data) == events_length == len(events)
        assert [common.event_from_sbs(i) for i in msg.data.data] == events

    await conn.async_close()
    await backend.async_close()
    await syncer_server.async_close()


# TODO rewrite
async def test_register_while_sync(conf):
    events = [common.Event(
        event_id=common.EventId(server=1,
                                session=2,
                                instance=i),
        event_type=('a', 'b', 'c'),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayload(common.EventPayloadType.JSON, i))
        for i in range(20)]
    sync_events = events[:10]
    register_events = events[10:]
    state_queue = aio.Queue()
    backend = create_backend(query_from_event_id_events=sync_events)
    syncer_server = await create_syncer_server(conf, backend)
    syncer_server.register_state_cb(state_queue.put_nowait)

    conn = await chatter.connect(common.sbs_repo, conf['address'])
    assert conn.is_open
    assert state_queue.empty()

    conn.send(chatter.Data(module='HatSyncer',
                           type='MsgReq',
                           data={'lastEventId': {'server': 1,
                                                 'session': 1,
                                                 'instance': 0},
                                 'clientName': 'abcd'}))

    state = await state_queue.get()
    assert len(state) == 1
    assert not state[0].synced

    msg = await conn.receive()
    assert msg.data.type == 'MsgEvents'
    assert [common.event_from_sbs(i) for i in msg.data.data] == sync_events

    for e in register_events:
        await backend.register([e])

    msg = await conn.receive()
    assert msg.data.type == 'MsgSynced'
    state = await state_queue.get()
    assert len(state) == 1
    assert state[0].synced

    for e in register_events:
        msg = await conn.receive()
        assert msg.data.type == 'MsgEvents'
        assert common.event_from_sbs(msg.data.data[0]) == e

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.01)

    await conn.async_close()
    await backend.async_close()
    await syncer_server.async_close()


async def test_multi_clients(conf):
    clients_count = 3
    events = [common.Event(
        event_id=common.EventId(server=1,
                                session=2,
                                instance=i),
        event_type=('a', 'b', 'c'),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayload(common.EventPayloadType.JSON, i))
        for i in range(10)]
    state_queue = aio.Queue()
    backend = create_backend(query_from_event_id_events=events)
    syncer_server = await create_syncer_server(conf, backend)
    syncer_server.register_state_cb(state_queue.put_nowait)

    conns = []
    for i in range(clients_count):
        conn = await chatter.connect(common.sbs_repo, conf['address'])
        last_event_id = common.EventId(
            server=1, session=0, instance=0)
        conn.client_name = f"cli{i}"
        conn.source = common.Source(type=common.SourceType.SYNCER,
                                    id=i + 1)
        conns.append(conn)
        conn.send(chatter.Data(module='HatSyncer',
                               type='MsgReq',
                               data={'lastEventId': last_event_id._asdict(),
                                     'clientName': conn.client_name}))

        state = await state_queue.get()
        assert len(state) == i + 1
        for client_info in state:
            if client_info.name == conn.client_name:
                assert not client_info.synced
            else:
                assert client_info.synced

        msg = await conn.receive()
        assert msg.data.type == 'MsgEvents'
        assert [common.event_from_sbs(i) for i in msg.data.data] == events

        msg = await conn.receive()
        assert msg.data.type == 'MsgSynced'
        assert msg.data.data is None

        state = await state_queue.get()
        assert len(state) == i + 1
        for client_info in state:
            assert client_info.synced

    for conn in conns:
        conn.close()

    for i, conn in enumerate(conns):
        conn.close()
        state = await state_queue.get()
        assert len(state) == clients_count - i - 1

    for conn in conns:
        await conn.async_close()

    await conn.async_close()
    await backend.async_close()
    await syncer_server.async_close()
