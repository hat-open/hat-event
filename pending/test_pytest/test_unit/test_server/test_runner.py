import asyncio

import pytest

from hat import aio
from hat import util

from hat.event.server import common
from hat.event.server.runner import EngineRunner


@pytest.fixture
def eventer_port():
    return util.get_unused_tcp_port()


@pytest.fixture
def eventer_address(eventer_port):
    return f'tcp+sbs://127.0.0.1:{eventer_port}'


@pytest.fixture
def conf(eventer_address):
    return {'engine': {'server_id': 1,
                       'modules': []},
            'eventer_server': {'address': eventer_address},
            'synced_restart_engine': False}


class Backend(aio.Resource):

    def __init__(self):
        self._async_group = aio.Group()
        self._events_cbs = util.CallbackRegistry()

    @property
    def async_group(self):
        return self._async_group

    def register_registered_events_cb(self, cb):
        return self._events_cbs.register(cb)

    def register_flushed_events_cb(self, cb):
        return self._events_cbs.register(cb)

    async def get_last_event_id(self, server_id):
        return common.EventId(server_id, 0, 0)

    async def register(self, events):
        self._events_cbs.notify(events)
        return events

    async def query(self, data):
        raise NotImplementedError()

    async def query_flushed(self, event_id):
        raise NotImplementedError()

    async def flush(self):
        pass


class SyncerServer(aio.Resource):

    def __init__(self):
        self._async_group = aio.Group()
        self._state = []
        self._state_cbs = util.CallbackRegistry()

    @property
    def async_group(self):
        return self._async_group

    @property
    def state(self):
        return self._state

    def register_state_cb(self, cb):
        return self._state_cbs.register(cb)

    async def flush(self):
        pass

    def set_state(self, state):
        self._state = state
        self._state_cbs.notify(state)


class SyncerClient(aio.Resource):

    def __init__(self):
        self._async_group = aio.Group()
        self._state_cbs = util.CallbackRegistry()
        self._events_cbs = util.CallbackRegistry()

    @property
    def async_group(self):
        return self._async_group

    def register_state_cb(self, cb):
        return self._state_cbs.register(cb)

    def register_events_cb(self, cb):
        return self._events_cbs.register(cb)

    def notify_state(self, server_id, state):
        self._state_cbs.notify(server_id, state)

    def notify_events(self, server_id, events):
        self._events_cbs.notify(server_id, events)


async def test_engine_runner_create(conf):
    backend = Backend()
    runner = EngineRunner(conf, backend, None, None)

    await asyncio.sleep(0.01)

    assert runner.is_open

    await runner.async_close()
    await backend.async_close()
