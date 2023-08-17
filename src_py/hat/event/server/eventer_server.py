"""Eventer server"""

import logging

from hat import aio
from hat.drivers import tcp

from hat.event import eventer
from hat.event.server import common
import hat.event.server.engine


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


async def create_eventer_server(addr: tcp.Address,
                                backend: common.Backend,
                                *,
                                server_token: str | None = None,
                                **kwargs
                                ) -> 'EventerServer':
    """Create eventer server"""
    server = EventerServer()
    server._backend = backend
    server._server_token = server_token
    server._engine = None

    server._srv = await eventer.listen(addr,
                                       connected_cb=server._on_connected,
                                       disconnected_cb=server._on_disconnected,
                                       register_cb=server._on_register,
                                       query_cb=server._on_query,
                                       **kwargs)

    return server


class EventerServer(aio.Resource):
    """Eventer server

    For creating new server see `create_eventer_server` coroutine.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group

    async def set_engine(self, engine: hat.event.server.engine.Engine | None):
        """Set engine"""
        self._engine = engine

        status = common.Status.OPERATIONAL if engine else common.Status.STANDBY
        await self._srv.set_status(status)

    async def notify_events(self,
                            events: list[common.Event],
                            persisted: bool):
        """Notify events"""
        await self._srv.notify_events(events, persisted)

    async def _on_connected(self, info):
        if (info.client_token is not None and
                info.client_token != self._server_token):
            raise Exception('invalid client token')

        if not self._engine or not self._engine.is_open:
            return

        source = _get_source(info.id)
        register_event = _create_eventer_event(info, 'CONNECTED')
        await self._engine.register(source, [register_event])

    async def _on_disconnected(self, info):
        if not self._engine or not self._engine.is_open:
            return

        source = _get_source(info.id)
        register_event = _create_eventer_event(info, 'DISCONNECTED')
        await self._engine.register(source, [register_event])

    async def _on_register(self, info, register_events):
        if not self._engine:
            return

        source = _get_source(info.id)
        return await self._engine.register(source, register_events)

    async def _on_query(self, info, params):
        return await self._backend.query(params)


def _create_eventer_event(info, status):
    return common.RegisterEvent(type=('event', 'eventer', info.client_name),
                                source_timestamp=None,
                                payload=common.EventPayloadJson(status))


def _get_source(source_id):
    return common.Source(type=common.SourceType.EVENTER,
                         id=source_id)
