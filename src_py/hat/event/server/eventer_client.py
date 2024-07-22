from collections.abc import Callable
import collections
import logging
import typing

from hat import aio
from hat.drivers import tcp

from hat.event import common
from hat.event import eventer


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

SyncedCb: typing.TypeAlias = Callable[[common.ServerId, int], None]
"""Synced callback"""


async def create_eventer_client(addr: tcp.Address,
                                client_name: str,
                                local_server_id: common.ServerId,
                                remote_server_id: common.ServerId,
                                backend: common.Backend,
                                *,
                                client_token: str | None = None,
                                synced_cb: SyncedCb | None = None,
                                **kwargs
                                ) -> 'EventerClient':
    """Create eventer client"""
    client = EventerClient()
    client._local_server_id = local_server_id
    client._remote_server_id = remote_server_id
    client._backend = backend
    client._synced_cb = synced_cb
    client._synced = False
    client._events_queue = collections.deque()

    client._client = await eventer.connect(addr=addr,
                                           client_name=client_name,
                                           client_token=client_token,
                                           subscriptions=[('*', )],
                                           server_id=remote_server_id,
                                           persisted=True,
                                           events_cb=client._on_events,
                                           **kwargs)

    try:
        client.async_group.spawn(client._synchronize)

    except BaseException:
        await aio.uncancellable(client.async_close())
        raise

    return client


class EventerClient(aio.Resource):
    """Eventer client

    For creating new client see `create_eventer_client` coroutine.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._client.async_group

    @property
    def synced(self) -> bool:
        """Synced state"""
        return self._synced

    async def _on_events(self, client, events):
        mlog.debug("received %s notify events", len(events))

        if self._events_queue is not None:
            self._events_queue.append(events)
            return

        await self._backend.register(events)

    async def _synchronize(self):
        mlog.debug("starting synchronization")

        try:
            last_event_id = await self._backend.get_last_event_id(
                self._remote_server_id)
            events = collections.deque()
            result = common.QueryResult([], True)
            synced_counter = 0

            await self._client.register([
                common.RegisterEvent(
                    type=('event', str(self._local_server_id), 'synced',
                          str(self._remote_server_id)),
                    source_timestamp=None,
                    payload=common.EventPayloadJson(False))])

            while result.more_follows:
                params = common.QueryServerParams(
                    server_id=self._remote_server_id,
                    persisted=True,
                    last_event_id=last_event_id)
                result = await self._client.query(params)

                mlog.debug("received %s query events", len(result.events))
                events.extend(result.events)
                synced_counter += len(result.events)
                if not events:
                    continue

                last_event_id = events[-1].id

                while events[0].id.session != events[-1].id.session:
                    session_id = events[0].id.session
                    session_events = collections.deque()

                    while events[0].id.session == session_id:
                        session_events.append(events.popleft())

                    await self._backend.register(session_events)

            if events:
                await self._backend.register(events)

            mlog.debug("processing cached notify events")
            while self._events_queue:
                events = [event for event in self._events_queue.popleft()
                          if event.id > last_event_id]
                if not events:
                    continue

                await self._backend.register(events)

            self._events_queue = None
            self._synced = True

            mlog.debug("synchronized %s events", synced_counter)

            await self._client.register([
                common.RegisterEvent(
                    type=('event', str(self._local_server_id), 'synced',
                          str(self._remote_server_id)),
                    source_timestamp=None,
                    payload=common.EventPayloadJson(True))])

            if self._synced_cb:
                await aio.call(self._synced_cb, self._remote_server_id,
                               synced_counter)

        except ConnectionError:
            mlog.debug("connection closed")
            self.close()

        except Exception as e:
            mlog.error("synchronization error: %s", e, exc_info=e)
            self.close()
