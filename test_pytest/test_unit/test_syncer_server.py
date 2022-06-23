import asyncio
import pytest

from hat import aio
from hat import chatter
from hat import util
from hat.event import common
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
    backend._register_cbs = util.CallbackRegistry()
    backend._query_from_event_id_events = query_from_event_id_events
    backend._from_event_id = None
    return backend


class Backend(aio.Resource):

    @property
    def async_group(self):
        return self._async_group

    def register_events_cb(self, cb):
        return self._register_cbs.register(cb)

    async def register(self, events):
        self._register_cbs.notify(events)
        return events

    async def query_from_event_id(self, event_id):
        self._from_event_id = event_id
        return self._query_from_event_id_events

    @property
    def from_event_id(self):
        return self._from_event_id


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
    client_state_queue = aio.Queue()

    def on_client_change(source, client_name, client_state):
        client_state_queue.put_nowait((source, client_name, client_state))

    backend = create_backend()
    syncer_server = await create_syncer_server(conf, backend)
    syncer_server.register_client_state_cb(on_client_change)

    conn = await chatter.connect(common.sbs_repo, conf['address'])
    assert conn.is_open
    assert client_state_queue.empty()

    conn.send(chatter.Data(module='HatSyncer',
                           type='MsgReq',
                           data={'lastEventId': None,
                                 'clientName': 'abcd',
                                 'subscription': []}))
    source, client_name, client_state = await client_state_queue.get()
    assert client_state == common.SyncerClientState.CONNECTED
    assert isinstance(source, common.Source)
    assert source.type == common.SourceType.SYNCER
    assert isinstance(source.id, int) and source.id > 0
    assert client_name == 'abcd'

    msg = await conn.receive()
    assert msg.data.type == 'MsgSynced'
    assert msg.data.data is None

    source, client_name, client_state = await client_state_queue.get()
    assert client_state == common.SyncerClientState.SYNCED
    assert client_name == 'abcd'
    assert source.type == common.SourceType.SYNCER

    conn.close()
    source, client_name, client_state = await client_state_queue.get()
    assert client_state == common.SyncerClientState.DISCONNECTED
    assert client_name == 'abcd'
    assert source.type == common.SourceType.SYNCER

    await conn.wait_closed()
    await backend.async_close()
    await syncer_server.async_close()


async def test_sync(conf):
    client_state_queue = aio.Queue()

    def on_client_change(source, client_name, client_state):
        client_state_queue.put_nowait((source, client_name, client_state))

    events = [common.Event(
        event_id=common.EventId(server=1,
                                session=2,
                                instance=i),
        event_type=('a', 'b', 'c'),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayload(common.EventPayloadType.JSON, i))
        for i in range(10)]
    backend = create_backend(query_from_event_id_events=events)
    syncer_server = await create_syncer_server(conf, backend)
    syncer_server.register_client_state_cb(on_client_change)

    conn = await chatter.connect(common.sbs_repo, conf['address'])
    assert conn.is_open
    assert client_state_queue.empty()

    last_event_id = common.EventId(server=1, session=123, instance=456)
    conn.send(chatter.Data(module='HatSyncer',
                           type='MsgReq',
                           data={'lastEventId': last_event_id._asdict(),
                                 'clientName': 'abcd',
                                 'subscription': []}))
    source, client_name, client_state = await client_state_queue.get()
    assert client_state == common.SyncerClientState.CONNECTED

    msg = await conn.receive()
    assert msg.data.type == 'MsgEvents'
    assert [common.event_from_sbs(i) for i in msg.data.data] == events
    assert backend.from_event_id == last_event_id

    msg = await conn.receive()
    assert msg.data.type == 'MsgSynced'
    assert msg.data.data is None
    _, _, client_state = await client_state_queue.get()
    assert client_state == common.SyncerClientState.SYNCED

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.01)

    await conn.async_close()
    await backend.async_close()
    await syncer_server.async_close()


async def test_register(conf):
    client_state_queue = aio.Queue()

    def on_client_change(source, client_name, client_state):
        client_state_queue.put_nowait((source, client_name, client_state))

    backend = create_backend()
    syncer_server = await create_syncer_server(conf, backend)
    syncer_server.register_client_state_cb(on_client_change)

    conn = await chatter.connect(common.sbs_repo, conf['address'])
    assert conn.is_open
    assert client_state_queue.empty()

    conn.send(chatter.Data(module='HatSyncer',
                           type='MsgReq',
                           data={'lastEventId': None,
                                 'clientName': 'abcd',
                                 'subscription': []}))

    msg = await conn.receive()
    assert msg.data.type == 'MsgSynced'
    _, _, client_state = client_state_queue.get_nowait_until_empty()
    assert client_state == common.SyncerClientState.SYNCED

    for events_length in [2, 4, 13]:
        events = [common.Event(
            event_id=common.EventId(server=1,
                                    session=2,
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


async def test_register_while_sync(conf):
    client_state_queue = aio.Queue()

    def on_client_change(source, client_name, client_state):
        client_state_queue.put_nowait((source, client_name, client_state))

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
    backend = create_backend(query_from_event_id_events=sync_events)
    syncer_server = await create_syncer_server(conf, backend)
    syncer_server.register_client_state_cb(on_client_change)

    conn = await chatter.connect(common.sbs_repo, conf['address'])
    assert conn.is_open
    assert client_state_queue.empty()

    conn.send(chatter.Data(module='HatSyncer',
                           type='MsgReq',
                           data={'lastEventId': None,
                                 'clientName': 'abcd',
                                 'subscription': []}))

    _, _, client_state = client_state_queue.get()
    assert client_state == common.SyncerClientState.CONNECTED

    for e in register_events:
        await backend.register([e])

    msg = await conn.receive()
    assert msg.data.type == 'MsgEvents'
    assert [common.event_from_sbs(i) for i in msg.data.data] == sync_events

    msg = await conn.receive()
    assert msg.data.type == 'MsgSynced'
    _, _, client_state = client_state_queue.get()
    assert client_state == common.SyncerClientState.SYNCED

    for e in register_events:
        msg = await conn.receive()
        assert msg.data.type == 'MsgEvents'
        assert common.event_from_sbs(msg.data.data[0]) == e

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.01)

    await conn.async_close()
    await backend.async_close()
    await syncer_server.async_close()
