import pytest

from hat import aio
from hat import util
from hat.drivers import tcp
import hat.monitor.common
import hat.monitor.component

from hat.event import common
import hat.event.server.eventer_client
import hat.event.server.eventer_server
import hat.event.server.engine_runner


class Backend(common.Backend):

    def __init__(self, register_cb=None):
        self._register_cb = register_cb
        self._async_group = aio.Group()

    @property
    def async_group(self):
        return self._async_group

    async def get_last_event_id(self, server_id):
        return common.EventId(server=server_id, session=0, instance=0)

    async def register(self, events):
        if self._register_cb:
            await aio.call(self._register_cb, events)

        return events

    async def query(self, params):
        return common.QueryResult([], False)

    async def flush(self):
        pass


class EventerServer(aio.Resource):

    def __init__(self, status_cb=None):
        self._status_cb = status_cb
        self._async_group = aio.Group()

    @property
    def async_group(self):
        return self._async_group

    def get_client_names(self):
        return []

    async def set_status(self, status, engine):
        if not self._status_cb:
            return

        await aio.call(self._status_cb, status, engine)

    async def notify_events(self, events, persisted, with_ack):
        pass


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_create():
    status_engine_queue = aio.Queue()
    conf = {'server_id': 123,
            'modules': []}

    def on_status(status, engine):
        status_engine_queue.put_nowait((status, engine))

    backend = Backend()
    eventer_server = EventerServer(status_cb=on_status)

    runner = hat.event.server.engine_runner.EngineRunner(
        conf=conf,
        backend=backend,
        eventer_server=eventer_server,
        reset_monitor_ready_cb=lambda: None)

    status, engine = await status_engine_queue.get()
    assert status == common.Status.STARTING
    assert engine is None

    status, engine = await status_engine_queue.get()
    assert status == common.Status.OPERATIONAL
    assert engine.is_open
    assert runner.is_open

    await runner.async_close()

    assert engine.is_closed

    await backend.async_close()
    await eventer_server.async_close()


async def test_close_engine():
    status_engine_queue = aio.Queue()
    conf = {'server_id': 123,
            'modules': []}

    def on_status(status, engine):
        status_engine_queue.put_nowait((status, engine))

    backend = Backend()
    eventer_server = EventerServer(status_cb=on_status)

    runner = hat.event.server.engine_runner.EngineRunner(
        conf=conf,
        backend=backend,
        eventer_server=eventer_server,
        reset_monitor_ready_cb=lambda: None)

    status, engine = await status_engine_queue.get()
    assert status == common.Status.STARTING
    assert engine is None

    status, engine = await status_engine_queue.get()
    assert status == common.Status.OPERATIONAL
    assert engine.is_open
    assert runner.is_open

    engine.close()

    await runner.wait_closed()

    assert engine.is_closed

    await backend.async_close()
    await eventer_server.async_close()


@pytest.mark.parametrize('state', hat.event.server.eventer_client.SyncedState)
@pytest.mark.parametrize('count', [None, 0, 1])
async def test_set_synced(state, count):
    status_engine_queue = aio.Queue()
    conf = {'server_id': 123,
            'modules': []}

    def on_status(status, engine):
        status_engine_queue.put_nowait((status, engine))

    events_queue = aio.Queue()
    backend = Backend(register_cb=events_queue.put_nowait)
    eventer_server = EventerServer(status_cb=on_status)

    runner = hat.event.server.engine_runner.EngineRunner(
        conf=conf,
        backend=backend,
        eventer_server=eventer_server,
        reset_monitor_ready_cb=lambda: None)

    status, engine = await status_engine_queue.get()
    assert status == common.Status.STARTING

    status, engine = await status_engine_queue.get()
    assert status == common.Status.OPERATIONAL

    assert engine.is_open

    events = await events_queue.get()
    event = events[0]
    assert event.type == ('event', str(conf['server_id']), 'engine')

    await runner.set_synced(server_id=42,
                            state=state,
                            count=count)

    events = await events_queue.get()
    event = events[0]
    assert event.type == ('event', str(conf['server_id']), 'synced', '42')
    assert event.payload.data['state'] == state.name

    if event.payload.data['state'] == 'SYNCED':
        event.payload.data['count'] == count

    else:
        assert 'count' not in event.payload.data

    await runner.async_close()
    await backend.async_close()
    await eventer_server.async_close()


async def test_reset():
    status_engine_queue = aio.Queue()
    conf = {'server_id': 123,
            'modules': []}

    def on_status(status, engine):
        status_engine_queue.put_nowait((status, engine))

    events_queue = aio.Queue()
    engine_queue = aio.Queue()
    backend = Backend(register_cb=events_queue.put_nowait)
    eventer_server = EventerServer(status_cb=on_status)

    runner = hat.event.server.engine_runner.EngineRunner(
        conf=conf,
        backend=backend,
        eventer_server=eventer_server,
        reset_monitor_ready_cb=lambda: None)

    status, engine = await status_engine_queue.get()
    assert status == common.Status.STARTING
    assert engine is None

    status, engine = await status_engine_queue.get()
    assert status == common.Status.OPERATIONAL
    assert engine.is_open

    assert status_engine_queue.empty()

    engine.restart()
    await engine.wait_closed()

    status, engine = await status_engine_queue.get()
    assert status == common.Status.STOPPING
    assert engine is None

    status, engine = await status_engine_queue.get()
    assert status == common.Status.STANDBY
    assert engine is None

    status, engine = await status_engine_queue.get()
    assert status == common.Status.STARTING
    assert engine is None

    status, engine = await status_engine_queue.get()
    assert status == common.Status.OPERATIONAL
    assert engine.is_open

    assert engine_queue.empty()

    assert runner.is_open

    await runner.async_close()
    await backend.async_close()
