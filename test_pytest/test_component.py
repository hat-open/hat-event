import pytest

from hat import aio
from hat import util
from hat.drivers import tcp
import hat.monitor.common
import hat.monitor.observer.server

import hat.event.component
import hat.event.eventer


@pytest.fixture
def observer_addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


@pytest.fixture
def eventer_addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_connect(observer_addr):
    name = 'component name'
    group = 'component group'
    server_group = 'server group'

    with pytest.raises(ConnectionError):
        await hat.event.component.connect(observer_addr, name, group,
                                          server_group, aio.Group)

    observer_server = await hat.monitor.observer.server.listen(observer_addr)
    component = await hat.event.component.connect(observer_addr, name, group,
                                                  server_group, aio.Group)

    assert component.is_open

    await component.async_close()
    await observer_server.async_close()


async def test_ready(observer_addr):
    name = 'component name'
    group = 'component group'
    server_group = 'server group'

    state_queue = aio.Queue()

    def on_state(server, state):
        state_queue.put_nowait(state)

    observer_server = await hat.monitor.observer.server.listen(
        observer_addr, state_cb=on_state)
    component = await hat.event.component.connect(observer_addr, name, group,
                                                  server_group, aio.Group)

    await state_queue.get()

    state = await state_queue.get()
    assert len(state.local_components) == 1
    info = state.local_components[0]
    assert info.name == name
    assert info.group == group
    assert info.blessing_res.ready is False

    await component.set_ready(True)

    state = await state_queue.get()
    assert len(state.local_components) == 1
    info = state.local_components[0]
    assert info.name == name
    assert info.group == group
    assert info.blessing_res.ready is True

    await component.async_close()
    await observer_server.async_close()


async def test_runner(observer_addr, eventer_addr):
    name = 'component name'
    group = 'component group'
    server_id = 123
    server_name = 'server name'
    server_group = 'server group'
    server_token = 'server token'

    state_queue = aio.Queue()
    runner_queue = aio.Queue()

    def on_state(server, state):
        state_queue.put_nowait(state)

    def on_runner(component, data, eventer_client):
        assert data == hat.event.component.ServerData(
            server_id=server_id,
            addr=eventer_addr,
            server_token=server_token)

        runner = aio.Group()
        runner_queue.put_nowait(runner)
        return runner

    eventer_server = await hat.event.eventer.listen(eventer_addr)
    observer_server = await hat.monitor.observer.server.listen(
        observer_addr, state_cb=on_state)
    component = await hat.event.component.connect(observer_addr, name, group,
                                                  server_group, on_runner)

    await state_queue.get()

    await state_queue.get()

    await component.set_ready(True)
    state = await state_queue.get()
    assert len(state.local_components) == 1
    component_info = state.local_components[0]

    server_info = hat.monitor.common.ComponentInfo(
        cid=123,
        mid=0,
        name=server_name,
        group=server_group,
        data={'server_id': server_id,
              'eventer_server': {'host': eventer_addr.host,
                                 'port': eventer_addr.port},
              'server_token': server_token},
        rank=1,
        blessing_req=hat.monitor.common.BlessingReq(token=123,
                                                    timestamp=321),
        blessing_res=hat.monitor.common.BlessingRes(token=123,
                                                    ready=True))

    blessing_req = hat.monitor.common.BlessingReq(token=12345,
                                                  timestamp=54321)

    await observer_server.update(0, [server_info, component_info._replace(
        blessing_req=blessing_req)])
    await state_queue.get()

    state = await state_queue.get()
    assert len(state.local_components) == 1
    component_info = state.local_components[0]

    assert component_info.blessing_res.token == blessing_req.token
    assert runner_queue.empty()

    await observer_server.update(0, [server_info, component_info])
    await state_queue.get()

    runner = await runner_queue.get()

    assert runner.is_open

    await observer_server.update(0, [component_info, server_info._replace(
        blessing_res=hat.monitor.common.BlessingRes(token=None,
                                                    ready=False))])
    await state_queue.get()

    await runner.wait_closed()

    await component.async_close()
    await observer_server.async_close()
    await eventer_server.async_close()


# TODO
