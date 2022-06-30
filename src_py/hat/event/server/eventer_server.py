"""Eventer server"""

import contextlib
import itertools
import logging

from hat import aio
from hat import chatter
from hat import json
from hat.event.server import common
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
    comm = EventerServer()
    comm._engine = engine
    comm._next_source_id = itertools.count(1)

    comm._server = await chatter.listen(sbs_repo=common.sbs_repo,
                                        address=conf['address'],
                                        connection_cb=comm._on_connection)
    mlog.debug("listening on %s", conf['address'])

    return comm


class EventerServer(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._server.async_group

    def _on_connection(self, conn):
        source_id = next(self._next_source_id)
        _Connection(conn, self._engine, source_id)


class _Connection(aio.Resource):

    def __init__(self, conn, engine, source_id):
        self._conn = conn
        self._engine = engine
        self._subscription = None
        self._source = common.Source(type=common.SourceType.EVENTER,
                                     id=source_id)

        self.async_group.spawn(self._connection_loop)

    @property
    def async_group(self):
        return self._conn.async_group

    def _on_events(self, events):
        if not self._subscription:
            return
        events = [e for e in events
                  if self._subscription.matches(e.event_type)]
        if not events:
            return

        data = chatter.Data('HatEventer', 'MsgNotify',
                            [common.event_to_sbs(e) for e in events])
        with contextlib.suppress(ConnectionError):
            self._conn.send(data)

    async def _connection_loop(self):
        mlog.debug("starting new client connection loop")
        try:
            with self._engine.register_events_cb(self._on_events):
                await self._register_eventer_event('CONNECTED')

                while True:
                    mlog.debug("waiting for incomming messages")
                    msg = await self._conn.receive()
                    msg_type = msg.data.module, msg.data.type

                    if msg_type == ('HatEventer', 'MsgSubscribe'):
                        mlog.debug("received subscribe message")
                        await self._process_msg_subscribe(msg)

                    elif msg_type == ('HatEventer', 'MsgRegisterReq'):
                        mlog.debug("received register request")
                        await self._process_msg_register(msg)

                    elif msg_type == ('HatEventer', 'MsgQueryReq'):
                        mlog.debug("received query request")
                        await self._process_msg_query(msg)

                    else:
                        raise Exception('unsupported message type')

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("connection loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("closing client connection loop")
            self.close()
            with contextlib.suppress(Exception):
                await self._register_eventer_event('DISCONNECTED')

    async def _process_msg_subscribe(self, msg):
        self._subscription = common.Subscription([tuple(i)
                                                  for i in msg.data.data])

    async def _process_msg_register(self, msg):
        register_events = [common.register_event_from_sbs(i)
                           for i in msg.data.data]
        events = await self._engine.register(self._source, register_events)
        if msg.last:
            return

        data = chatter.Data(module='HatEventer',
                            type='MsgRegisterRes',
                            data=[(('event', common.event_to_sbs(e))
                                   if e is not None else ('failure', None))
                                  for e in events])
        self._conn.send(data, conv=msg.conv)

    async def _process_msg_query(self, msg):
        query_data = common.query_from_sbs(msg.data.data)
        events = await self._engine.query(query_data)
        data = chatter.Data(module='HatEventer',
                            type='MsgQueryRes',
                            data=[common.event_to_sbs(e) for e in events])
        self._conn.send(data, conv=msg.conv)

    async def _register_eventer_event(self, status):
        register_event = common.RegisterEvent(
            event_type=('event', 'eventer'),
            source_timestamp=None,
            payload=common.EventPayload(type=common.EventPayloadType.JSON,
                                        data=status))
        await self._engine.register(self._source, [register_event])
