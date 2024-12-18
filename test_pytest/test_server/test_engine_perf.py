import pytest

from hat import aio

from hat.event import common
import hat.event.backends.dummy
import hat.event.server.engine


pytestmark = pytest.mark.perf


class Module(common.Module):

    def __init__(self, conf, engine, source):
        self._async_group = aio.Group()
        self._subscription = common.create_subscription(
            tuple(i) for i in conf['subscriptions'])
        self._recursion_count = conf['recursion_count']
        self._counter = 0

    @property
    def async_group(self):
        return self._async_group

    @property
    def subscription(self):
        return self._subscription

    def on_session_start(self, session_id):
        self._counter = self._recursion_count

    def process(self, source, event):
        if self._counter < 1:
            return

        self._counter -= 1
        yield common.RegisterEvent(type=event.type,
                                   source_timestamp=event.source_timestamp,
                                   payload=event.payload)


info = common.ModuleInfo(Module)


class EventerServer(aio.Resource):

    def __init__(self):
        self._async_group = aio.Group()

    @property
    def async_group(self):
        return self._async_group

    def get_client_names(self):
        raise NotImplementedError()

    async def set_status(self, status, engine):
        raise NotImplementedError()

    async def notify_events(self, events, persisted, with_ack):
        raise NotImplementedError()


@pytest.mark.parametrize("recursion_count", [0, 1, 5, 10])
@pytest.mark.parametrize("module_count", [1, 10, 100, 1000])
async def test_register(duration, recursion_count, module_count):
    event_types = [('a', 'b', str(i))
                   for i in range(module_count)]

    module_confs = [{'module': __name__,
                     'subscriptions': [list(event_type)],
                     'recursion_count': recursion_count}
                    for event_type in event_types]

    backend = hat.event.backends.dummy.DummyBackend(None, None, None)
    eventer_server = EventerServer()
    server_id = 1

    engine = await hat.event.server.engine.create_engine(
        backend=backend,
        eventer_server=eventer_server,
        module_confs=module_confs,
        server_id=server_id,
        restart_cb=lambda: None,
        reset_monitor_ready_cb=lambda: None)

    description = (f'recursion_count: {recursion_count}; '
                   f'module_count: {module_count}')

    source = common.Source(type=common.SourceType.EVENTER,
                           id=1)
    events = [common.RegisterEvent(type=event_type,
                                   source_timestamp=None,
                                   payload=None)
              for event_type in event_types]

    with duration(description):
        await engine.register(source=source,
                              events=events)

    await engine.async_close()
    await eventer_server.async_close()
    await backend.async_close()
