import typing

from hat import aio

from hat.event.syncer import common


SyncedCb = aio.AsyncCallable[['Client'], None]

EventsCb = aio.AsyncCallable[['Client', typing.List[common.Event]], None]


async def connect(address: str,
                  client_name: str,
                  last_event_id: common.EventId,
                  syncer_token: typing.Optional[str] = None,
                  synced_cb: typing.Optional[SyncedCb] = None,
                  events_cb: typing.Optional[EventsCb] = None
                  ) -> 'Client':
    pass


class Client(aio.Resource):

    @property
    def async_group(self):
        return self._conn.async_group
