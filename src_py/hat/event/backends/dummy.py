"""Dummy backend

Simple backend which returns constant values where:

    * `DummyBackend.get_last_event_id` returns session and instance ``0``
    * `DummyBackend.register` returns input arguments
    * `DummyBackend.query` returns ``QueryResult([], False)``

Registered and flushed events callback is called on every register.

"""

from hat import aio

from hat.event import common


class DummyBackend(common.Backend):

    def __init__(self, conf, registered_events_cb, flushed_events_cb):
        self._registered_events_cbs = registered_events_cb
        self._flushed_events_cbs = flushed_events_cb
        self._async_group = aio.Group()

    @property
    def async_group(self):
        return self._async_group

    async def get_last_event_id(self, server_id):
        return common.EventId(server_id, 0, 0)

    async def register(self, events):
        if self._registered_events_cbs:
            await aio.call(self._registered_events_cbs, events)

        if self._flushed_events_cbs:
            await aio.call(self._flushed_events_cbs, events)

        return events

    async def query(self, params):
        return common.QueryResult(events=[],
                                  more_follows=False)

    async def flush(self):
        pass


info = common.BackendInfo(DummyBackend)
