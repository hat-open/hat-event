import pytest

from hat import aio
from hat import util
from hat.drivers import tcp
import hat.monitor.common
import hat.monitor.component

from hat.event import common
import hat.event.server.engine
import hat.event.server.eventer_client
import hat.event.server.eventer_client_runner
import hat.event.server.eventer_server


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


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_create():
    conf = {'server_id': 1,
            'name': 'event server name',
            'monitor_component': {'group': 'event server group'}}

    async def on_synced(server_id, state, count):
        pass

    backend = Backend()
    runner = hat.event.server.eventer_client_runner.EventerClientRunner(
        conf=conf,
        backend=backend,
        synced_cb=on_synced)

    assert runner.is_open

    await runner.async_close()
    await backend.async_close()


async def test_set_monitor_state(addr):
    conf = {'server_id': 1,
            'name': 'name1',
            'monitor_component': {'group': 'group1'}}
    synced_queue = aio.Queue()

    async def on_synced(server_id, state, count):
        synced_queue.put_nowait((server_id, state, count))

    backend = Backend()
    eventer_server = await hat.event.server.eventer_server.create_eventer_server(  # NOQA
        addr=addr,
        backend=backend,
        server_id=2)
    runner = hat.event.server.eventer_client_runner.EventerClientRunner(
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
    runner.set_monitor_state(state)

    server_id, state, count = await synced_queue.get()
    assert server_id == 42
    assert state == hat.event.server.eventer_client.SyncedState.CONNECTED
    assert count is None

    server_id, state, count = await synced_queue.get()
    assert server_id == 42
    assert state == hat.event.server.eventer_client.SyncedState.SYNCED
    assert count == 0

    state = hat.monitor.component.State(info=None,
                                        components=[])
    runner.set_monitor_state(state)

    assert runner.is_open

    await runner.async_close()
    await eventer_server.async_close()
    await backend.async_close()


async def test_operational(addr):
    conf = {'server_id': 1,
            'name': 'name1',
            'monitor_component': {'group': 'group1'}}
    operational_queue = aio.Queue()
    synced_queue = aio.Queue()

    async def on_synced(server_id, state, count):
        synced_queue.put_nowait((server_id, state, count))

    backend = Backend()
    eventer_server = await hat.event.server.eventer_server.create_eventer_server(  # NOQA
        addr=addr,
        backend=backend,
        server_id=2)
    engine = await hat.event.server.engine.create_engine(
        backend=backend,
        eventer_server=eventer_server,
        module_confs=[],
        server_id=2,
        restart_cb=lambda: None,
        reset_monitor_ready_cb=lambda: None)
    runner = hat.event.server.eventer_client_runner.EventerClientRunner(
        conf=conf,
        backend=backend,
        synced_cb=on_synced)
    runner.register_operational_cb(operational_queue.put_nowait)

    assert runner.operational is False
    assert operational_queue.empty()

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
    runner.set_monitor_state(state)

    server_id, state, count = await synced_queue.get()
    assert state == hat.event.server.eventer_client.SyncedState.CONNECTED

    server_id, state, count = await synced_queue.get()
    assert state == hat.event.server.eventer_client.SyncedState.SYNCED

    assert runner.operational is False
    assert operational_queue.empty()

    await eventer_server.set_status(common.Status.OPERATIONAL, engine)

    operational = await operational_queue.get()
    assert operational is True
    assert runner.operational is True
    assert operational_queue.empty()

    await eventer_server.set_status(common.Status.STANDBY, None)

    operational = await operational_queue.get()
    assert operational is False
    assert runner.operational is False
    assert operational_queue.empty()

    await runner.async_close()
    await engine.async_close()
    await eventer_server.async_close()
    await backend.async_close()
