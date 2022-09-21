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


class ClientInfo(typing.NamedTuple):
    """Client connection information"""
    name: str
    synced: bool


StateCb = typing.Callable[[typing.List[ClientInfo]], None]
"""Syncer state change callback"""


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
    srv._state = {}
    srv._next_client_ids = itertools.count(1)
    srv._state_cbs = util.CallbackRegistry()

    srv._server = await chatter.listen(sbs_repo=common.sbs_repo,
                                       address=conf['address'],
                                       connection_cb=srv._on_connection)
    mlog.debug("listening on %s", conf['address'])
    return srv


class SyncerServer(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._server.async_group

    @property
    def state(self) -> typing.Iterable[ClientInfo]:
        """State of all active connections"""
        return self._state.values()

    def register_state_cb(self,
                          cb: StateCb
                          ) -> util.RegisterCallbackHandle:
        """Register state change callback"""
        return self._state_cbs.register(cb)

    def _on_connection(self, conn):
        conn.async_group.spawn(self._connection_loop, conn)

    def _update_client_info(self, client_id, name, synced):
        self._state[client_id] = ClientInfo(name, synced)
        self._state_cbs.notify(list(self._state.values()))

    def _remove_client_info(self, client_id):
        if self._state.pop(client_id, None):
            self._state_cbs.notify(list(self._state.values()))

    async def _connection_loop(self, conn):
        mlog.debug("starting new client connection loop")

        client_id = None
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
            client_id = next(self._next_client_ids)
            self._update_client_info(client_id, name, False)

            events_queue = aio.Queue()
            with self._backend.register_flushed_events_cb(
                    events_queue.put_nowait):
                mlog.debug("query backend")
                async for events in self._backend.query_flushed(
                        msg_req.last_event_id):
                    last_event_id = events[-1].event_id
                    data = chatter.Data(module='HatSyncer',
                                        type='MsgEvents',
                                        data=[common.event_to_sbs(e)
                                              for e in events])
                    conn.send(data)

                self._update_client_info(client_id, name, True)
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
            self._remove_client_info(client_id)


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
