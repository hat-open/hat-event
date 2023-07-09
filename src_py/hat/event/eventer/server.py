import contextlib
import itertools
import logging
import typing

from hat import aio
from hat import chatter

from hat.event import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

ClientId: typing.TypeAlias = int
"""Client identifier"""

ClientCb: typing.TypeAlias = aio.AsyncCallable[[ClientId], None]
"""Client connected/disconnected callback"""

RegisterCb: typing.TypeAlias = aio.AsyncCallable[[ClientId,
                                                  list[common.RegisterEvent]],
                                                 list[common.Event]]
"""Register callback"""

QueryCb: typing.TypeAlias = aio.AsyncCallable[[ClientId,
                                               list[common.QueryData]],
                                              list[common.Event]]
"""Query callback"""


async def listen(address: str,
                 connected_cb: ClientCb | None = None,
                 disconnected_cb: ClientCb | None = None,
                 register_cb: RegisterCb | None = None,
                 query_cb: QueryCb | None = None
                 ) -> 'Server':
    """Create eventer server instance"""
    server = Server()
    server._connected_cb = connected_cb
    server._disconnected_cb = disconnected_cb
    server._register_cb = register_cb
    server._query_cb = query_cb
    server._next_client_ids = itertools.count(1)
    server._conns = {}

    server._srv = await chatter.listen(sbs_repo=common.sbs_repo,
                                       address=address,
                                       connection_cb=server._on_connection)
    mlog.debug("listening on %s", address)

    return server


class Server(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group

    def notify(self, events: list[common.Event]):
        """Notify events to subscribed clients"""
        for conn in self._conns.values():
            conn.notify(events)

    async def _on_connection(self, conn):
        client_id = next(self._next_client_ids)

        try:
            if self._connected_cb:
                await aio.call(self._connected_cb, client_id)

            self._conns[client_id] = _Connection(conn=conn,
                                                 client_id=client_id,
                                                 register_cb=self._register_cb,
                                                 query_cb=self._query_cb)

            await self._conns[client_id].wait_closing()

        except Exception as e:
            mlog.error("on connection error: %s", e, exc_info=e)

        finally:
            conn.close()

            if self._disconnected_cb:
                with contextlib.suppress(Exception):
                    await aio.call(self._disconnected_cb, client_id)


class _Connection(aio.Resource):

    def __init__(self,
                 conn: chatter.Connection,
                 client_id: int,
                 register_cb: RegisterCb | None,
                 query_cb: QueryCb | None):
        self._conn = conn
        self._client_id = client_id
        self._register_cb = register_cb
        self._query_cb = query_cb
        self._subscription = None

        self.async_group.spawn(self._connection_loop)

    @property
    def async_group(self) -> aio.Group:
        return self._conn.async_group

    def notify(self, events: list[common.Event]):
        if not self.is_open or not self._subscription:
            return

        events = [event for event in events
                  if self._subscription.matches(event.event_type)]

        if not events:
            return

        data = chatter.Data('HatEventer', 'MsgNotify',
                            [common.event_to_sbs(e) for e in events])
        with contextlib.suppress(ConnectionError):
            self._conn.send(data)

    async def _connection_loop(self):
        mlog.debug("starting connection loop")
        try:
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
            mlog.debug("closing connection loop")
            self._conn.close()

    async def _process_msg_subscribe(self, msg):
        self._subscription = common.Subscription([tuple(i)
                                                  for i in msg.data.data])

    async def _process_msg_register(self, msg):
        register_events = [common.register_event_from_sbs(i)
                           for i in msg.data.data]

        if self._register_cb:
            events = await aio.call(self._register_cb, self._client_id,
                                    register_events)

        else:
            events = [None for _ in register_events]

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

        if self._query_cb:
            events = await aio.call(self._query_cb, self._client_id,
                                    query_data)

        else:
            events = []

        data = chatter.Data(module='HatEventer',
                            type='MsgQueryRes',
                            data=[common.event_to_sbs(e) for e in events])
        self._conn.send(data, conv=msg.conv)
