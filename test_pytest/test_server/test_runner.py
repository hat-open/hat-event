import asyncio

import pytest

from hat import aio
from hat import util
from hat.drivers import tcp
import hat.monitor.common
import hat.monitor.component

from hat.event import common
import hat.event.server.eventer_server
import hat.event.server.runner


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

    def __init__(self, engine_cb=None):
        self._engine_cb = engine_cb
        self._async_group = aio.Group()

    @property
    def async_group(self):
        return self._async_group

    async def set_engine(self, engine):
        if not self._engine_cb:
            return

        await aio.call(self._engine_cb, engine)

    async def notify_events(self, events, persisted):
        pass


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_engine_runner_create():
    conf = {'server_id': 123,
            'modules': []}

    engine_queue = aio.Queue()
    backend = Backend()
    eventer_server = EventerServer(engine_cb=engine_queue.put_nowait)

    runner = hat.event.server.runner.EngineRunner(
        conf=conf,
        backend=backend,
        eventer_server=eventer_server)

    engine = await engine_queue.get()

    assert engine.is_open
    assert runner.is_open

    await runner.async_close()

    assert engine.is_closed

    await backend.async_close()
    await eventer_server.async_close()


async def test_engine_runner_close_engine():
    conf = {'server_id': 123,
            'modules': []}

    engine_queue = aio.Queue()
    backend = Backend()
    eventer_server = EventerServer(engine_cb=engine_queue.put_nowait)

    runner = hat.event.server.runner.EngineRunner(
        conf=conf,
        backend=backend,
        eventer_server=eventer_server)

    engine = await engine_queue.get()

    assert engine.is_open
    assert runner.is_open

    engine.close()

    await runner.wait_closed()

    assert engine.is_closed

    await backend.async_close()
    await eventer_server.async_close()


@pytest.mark.parametrize('synced_restart_engine', [True, False])
@pytest.mark.parametrize('synced', [True, False])
@pytest.mark.parametrize('counter', [0, 1])
async def test_engine_runner_set_synced(synced_restart_engine, synced,
                                        counter):
    conf = {'server_id': 123,
            'modules': [],
            'synced_restart_engine': synced_restart_engine}

    events_queue = aio.Queue()
    engine_queue = aio.Queue()
    backend = Backend(register_cb=events_queue.put_nowait)
    eventer_server = EventerServer(engine_cb=engine_queue.put_nowait)

    runner = hat.event.server.runner.EngineRunner(
        conf=conf,
        backend=backend,
        eventer_server=eventer_server)

    engine = await engine_queue.get()
    assert engine.is_open

    events = await events_queue.get()
    event = events[0]
    assert event.type == ('event', str(conf['server_id']), 'engine')

    await runner.set_synced(server_id=42,
                            synced=synced,
                            counter=counter)

    events = await events_queue.get()
    event = events[0]
    assert event.type == ('event', str(conf['server_id']), 'synced', '42')
    assert event.payload.data == synced

    if synced_restart_engine and synced and counter:
        await engine.wait_closed()

        engine = await engine_queue.get()
        assert engine is None

        engine = await engine_queue.get()
        assert engine.is_open

    else:
        with pytest.raises(asyncio.TimeoutError):
            await aio.wait_for(engine.wait_closing(), 0.01)

        assert engine.is_open

    await runner.async_close()
    await backend.async_close()
    await eventer_server.async_close()


async def test_eventer_client_runner_create():
    conf = {'server_id': 1,
            'monitor_component': {'name': 'event server name',
                                  'group': 'event server group'}}

    async def on_synced(server_id, synced, counter):
        pass

    backend = Backend()
    runner = hat.event.server.runner.EventerClientRunner(
        conf=conf,
        backend=backend,
        synced_cb=on_synced)

    assert runner.is_open

    await runner.async_close()
    await backend.async_close()


async def test_eventer_client_runner_set_monitor_state(addr):
    conf = {'server_id': 1,
            'monitor_component': {'name': 'name1',
                                  'group': 'group1'}}
    synced_queue = aio.Queue()

    async def on_synced(server_id, synced, counter):
        synced_queue.put_nowait((server_id, synced, counter))

    backend = Backend()
    eventer_server = await hat.event.server.eventer_server.create_eventer_server(  # NOQA
        addr=addr,
        backend=backend,
        server_id=2)
    runner = hat.event.server.runner.EventerClientRunner(
        conf=conf,
        backend=backend,
        synced_cb=on_synced)

    assert synced_queue.empty()

    blessing_req = hat.monitor.common.BlessingReq(token=123,
                                                  timestamp=None)
    blessing_res = hat.monitor.common.BlessingRes(token=123,
                                                  ready=True)
    data = {'server_id': 42,
            'eventer_server': {'host': addr.host,
                               'port': addr.port},
            'server_token': None}
    info = hat.monitor.common.ComponentInfo(cid=1,
                                            mid=2,
                                            name='name2',
                                            group='group1',
                                            data=data,
                                            rank=1,
                                            blessing_req=blessing_req,
                                            blessing_res=blessing_res)
    state = hat.monitor.component.State(info=None,
                                        components=[info])
    await runner.set_monitor_state(state)

    server_id, synced, counter = await synced_queue.get()
    assert server_id == 42
    assert synced is False

    server_id, synced, counter = await synced_queue.get()
    assert server_id == 42
    assert synced is True
    assert counter == 0

    state = hat.monitor.component.State(info=None,
                                        components=[])
    await runner.set_monitor_state(state)

    assert runner.is_open

    await runner.async_close()
    await eventer_server.async_close()
    await backend.async_close()


# TODO MainRunner
