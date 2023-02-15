"""Eventer server"""

import logging

from hat import aio
from hat import json

from hat.event.server import common
import hat.event.eventer
import hat.event.server.engine


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


async def create_eventer_server(conf: json.Data,
                                engine: hat.event.server.engine.Engine
                                ) -> 'EventerServer':
    """Create eventer server

    Args:
        conf: configuration defined by
            ``hat-event://main.yaml#/definitions/eventer_server``
        engine: engine

    """

    async def on_connected(client_id):
        await _register_eventer_event(engine, client_id, 'CONNECTED')

    async def on_disconnected(client_id):
        await _register_eventer_event(engine, client_id, 'DISCONNECTED')

    async def on_register(client_id, register_events):
        return await engine.register(_get_source(client_id), register_events)

    async def on_query(client_id, query_data):
        return await engine.query(query_data)

    server = EventerServer()
    server._srv = await hat.event.eventer.listen(
        address=conf['address'],
        connected_cb=on_connected,
        disconnected_cb=on_disconnected,
        register_cb=on_register,
        query_cb=on_query)

    handler = engine.register_events_cb(server._srv.notify)
    server.async_group.spawn(aio.call_on_cancel, handler.cancel)

    return server


class EventerServer(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group


def _get_source(client_id):
    return common.Source(type=common.SourceType.EVENTER,
                         id=client_id)


async def _register_eventer_event(engine, client_id, status):
    register_event = common.RegisterEvent(
        event_type=('event', 'eventer'),
        source_timestamp=None,
        payload=common.EventPayload(type=common.EventPayloadType.JSON,
                                    data=status))
    await engine.register(_get_source(client_id), [register_event])
