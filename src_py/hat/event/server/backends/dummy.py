"""Dummy backend

Simple backend which returns constat values where:

    * `DummyBackend.get_last_event_id` returns session and instance ``0``
    * `DummyBackend.register` returns input arguments
    * `DummyBackend.query` returns ``[]``
    * `DummyBackend.query_flushed` returns empty iterable

Registered flushed events callback is never notified.

"""

import typing

from hat import aio
from hat import json
from hat import util
from hat.event.server import common


json_schema_id = None
json_schema_repo = None


async def create(conf: json.Data) -> 'DummyBackend':
    backend = DummyBackend()
    backend._async_group = aio.Group()
    return backend


class DummyBackend(common.Backend):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    def register_flushed_events_cb(self,
                                   cb: typing.Callable[[typing.List[common.Event]],  # NOQA
                                                       None]
                                   ) -> util.RegisterCallbackHandle:
        return util.RegisterCallbackHandle(cancel=lambda: None)

    async def get_last_event_id(self,
                                server_id: int
                                ) -> common.EventId:
        return common.EventId(server_id, 0, 0)

    async def register(self,
                       events: typing.List[common.Event]
                       ) -> typing.List[typing.Optional[common.Event]]:
        return events

    async def query(self,
                    data: common.QueryData
                    ) -> typing.List[common.Event]:
        return []

    async def query_flushed(self,
                            data: common.QueryData
                            ) -> typing.AsyncIterable[typing.List[common.Event]]:  # NOQA
        for events in []:
            yield events
