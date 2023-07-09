"""Mariner server"""

import logging
import urllib.parse

from hat import aio
from hat import json
from hat.drivers import tcp

from hat.event import mariner
from hat.event.server import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


async def create_mariner_server(conf: json.Data,
                                backend: common.Backend
                                ) -> 'MarinerServer':
    """Create mariner server

    Args:
        conf: configuration defined by
            ``hat-event://main.yaml#/definitions/mariner_server``
        backend: backend

    """
    url = urllib.parse.urlparse(conf['address'])
    address = tcp.Address(url.hostname, url.port)

    server = MarinerServer()
    server._backend = backend

    server._server = await mariner.listen(
        address, server._on_connection,
        subscriptions=[tuple(event_type)
                       for event_type in conf['subscriptions']])

    return server


class MarinerServer(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._server.async_group

    async def _on_connection(self, conn):
        try:
            mlog.debug("new connection: %s", conn.client_id)

            # TODO check client token, subscriptions

            conn = _Connection(self._backend, conn)

        except Exception as e:
            mlog.error("on connection error: %s", e, exc_info=e)
            conn.close()

        # await conn.wait_closing()


class _Connection(aio.Resource):

    def __init__(self,
                 backend: common.Backend,
                 conn: mariner.ServerConnection):
        self._backend = backend
        self._conn = conn
        self._last_event_ids = ({conn.last_event_id.server: conn.last_event_id}
                                if conn.last_event_id else {})
        self._subscription = self._conn.subscription

        self.async_group.spawn(self._connection_loop)

    @property
    def async_group(self) -> aio.Group:
        return self._conn.async_group

    async def _connection_loop(self):
        try:
            mlog.debug("starting connection loop")

            events_queue = aio.Queue()
            with self._backend.register_flushed_events_cb(
                    events_queue.put_nowait):

                for last_event_id in self._last_event_ids.values():
                    async for events in self._backend.query_flushed(
                            last_event_id):
                        await self._send_events(events)

                while True:
                    events = await events_queue.get()
                    await self._send_events(events)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("connection loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping connection loop")
            self.close()

    async def _send_events(self, events):
        if not events:
            return

        last_event_id = self._last_event_ids.get(events[0].event_id.server)
        if last_event_id:
            if events[0].event_id.session < last_event_id.session:
                return

            if events[0].event_id.session == last_event_id.session:
                events = (event for event in events
                          if event.event_id > last_event_id)

        events = [event for event in events
                  if self._subscription.matches(event.event_type)]
        if not events:
            return

        mlog.debug("sending events")
        await self._conn.send_events(events)

        last_event_id = events[-1].event_id
        self._last_event_ids[last_event_id.server] = last_event_id
