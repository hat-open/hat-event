"""Syncer server"""

import itertools
import logging
import typing

from hat import aio
from hat import chatter
from hat import json
from hat import util
from hat.event.server import common
import hat.event.common.data


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


async def create_syncer_server(conf: json.Data,
                               backend: common.Backend
                               ) -> 'SyncerServer':
    """Create syncer server

    Args:
        conf: configuration defined by
            ``hat-event://main.yaml#/definitions/syncer_server``
        backend: backend

    """
    srv = SyncerServer()
    srv._backend = backend
    srv._next_source_id = itertools.count(1)
    srv._client_state_cbs = util.CallbackRegistry()

    srv._server = await chatter.listen(sbs_repo=common.sbs_repo,
                                       address=conf['address'],
                                       connection_cb=srv._on_connection)
    mlog.debug("listening on %s", conf['address'])
    return srv


StateCb = typing.Callable[[common.Source, str, common.SyncerClientState], None]
"""Syncer client state callback

Callback is called with source identifier, syncer client name and current
syncer state

"""


class SyncerServer(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._server.async_group

    def register_client_state_cb(self,
                                 cb: StateCb
                                 ) -> util.RegisterCallbackHandle:
        """Register client state callback"""
        return self._client_state_cbs.register(cb)

    def _on_connection(self, conn):
        source_id = next(self._next_source_id)
        conn.async_group.spawn(_run_connection_loop, conn,
                               self._client_state_cbs.notify, self._backend,
                               source_id)


async def _run_connection_loop(conn, notify_client_state, backend, source_id):
    mlog.debug("starting new client connection loop")

    name = None
    source = common.Source(type=common.SourceType.SYNCER,
                           id=source_id)

    try:
        mlog.debug("waiting for incomming message")
        msg = await conn.receive()
        msg_type = msg.data.module, msg.data.type

        if msg_type != ('HatSyncer', 'MsgReq'):
            raise Exception('unsupported message type')

        mlog.debug("received request")
        conn.async_group.spawn(_receive_loop, conn)

        msg_req = hat.event.common.data.syncer_req_from_sbs(msg.data.data)
        last_event_id = msg_req.last_event_id
        name = msg_req.client_name
        notify_client_state(source, name, common.SyncerClientState.CONNECTED)

        events_queue = aio.Queue()
        with backend.register_flushed_events_cb(events_queue.put_nowait):
            mlog.debug("query backend")
            async for events in backend.query_flushed(
                    msg_req.last_event_id):
                last_event_id = events[-1].event_id
                data = chatter.Data(module='HatSyncer',
                                    type='MsgEvents',
                                    data=[common.event_to_sbs(e)
                                          for e in events])
                conn.send(data)

            notify_client_state(source, name, common.SyncerClientState.SYNCED)
            conn.send(chatter.Data(module='HatSyncer',
                                   type='MsgSynced',
                                   data=None))

            while True:
                events = await events_queue.get()

                if not events:
                    continue

                if events[0].event_id.server != last_event_id.server:
                    continue

                if events[0].event_id.session < last_event_id.session:
                    continue

                if events[0].event_id.session == last_event_id.session:
                    events = [event for event in events
                              if event.event_id > last_event_id]

                data = chatter.Data(module='HatSyncer',
                                    type='MsgEvents',
                                    data=[common.event_to_sbs(e)
                                          for e in events])
                conn.send(data)

    except ConnectionError:
        pass

    except Exception as e:
        mlog.error("connection loop error: %s", e, exec_info=e)

    finally:
        mlog.debug("closing client connection loop")
        conn.close()
        if name:
            notify_client_state(source, name,
                                common.SyncerClientState.DISCONNECTED)


async def _receive_loop(conn):
    try:
        await conn.receive()
        raise Exception("unexpected request")

    except ConnectionError:
        pass

    except Exception as e:
        mlog.error("receive loop error: %s", e, exec_info=e)

    finally:
        conn.close()
