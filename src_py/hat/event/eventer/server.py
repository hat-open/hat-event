from collections.abc import Collection
import asyncio
import collections
import contextlib
import itertools
import logging
import typing

from hat import aio
from hat.drivers import chatter
from hat.drivers import tcp

from hat.event.eventer import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

ConnectionId: typing.TypeAlias = int
"""Connection identifier"""


class ConnectionInfo(typing.NamedTuple):
    id: ConnectionId
    client_name: str
    client_token: str | None
    subscription: common.Subscription
    server_id: int | None
    persisted: bool


ConnectionCb: typing.TypeAlias = aio.AsyncCallable[[ConnectionInfo], None]
"""Connected/disconnected callback"""

RegisterCb: typing.TypeAlias = aio.AsyncCallable[
    [ConnectionInfo, Collection[common.RegisterEvent]],
    Collection[common.Event] | None]
"""Register callback"""

QueryCb: typing.TypeAlias = aio.AsyncCallable[
    [ConnectionInfo, common.QueryParams],
    common.QueryResult]
"""Query callback"""


async def listen(addr: tcp.Address,
                 *,
                 status: common.Status = common.Status.STANDBY,
                 connected_cb: ConnectionCb | None = None,
                 disconnected_cb: ConnectionCb | None = None,
                 register_cb: RegisterCb | None = None,
                 query_cb: QueryCb | None = None,
                 close_timeout: float = 0.5,
                 **kwargs
                 ) -> 'Server':
    """Create listening Eventer Server instance"""
    server = Server()
    server._status = status
    server._connected_cb = connected_cb
    server._disconnected_cb = disconnected_cb
    server._register_cb = register_cb
    server._query_cb = query_cb
    server._close_timeout = close_timeout
    server._loop = asyncio.get_running_loop()
    server._next_conn_ids = itertools.count(1)
    server._conn_infos = {}
    server._conn_conv_futures = {}

    server._srv = await chatter.listen(server._connection_loop, addr, **kwargs)
    mlog.debug("listening on %s", addr)

    return server


class Server(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group

    async def set_status(self, status: common.Status):
        """Set status and wait for acks"""
        if self._status == status:
            return

        self._status = status
        tasks = [self.async_group.spawn(self._notify_status, conn, status)
                 for conn in self._conn_infos.keys()]
        if not tasks:
            return

        await asyncio.wait(tasks)

    async def notify_events(self,
                            events: Collection[common.Event],
                            persisted: bool):
        """Notify events to clients"""
        for conn, info in list(self._conn_infos.items()):
            if info.persisted != persisted:
                continue

            filtered_events = collections.deque(
                event for event in events
                if (info.subscription.matches(event.type) and
                    (info.server_id is None or
                     info.server_id == event.id.server)))
            if not filtered_events:
                continue

            await self._notify_events(conn, filtered_events)

    async def _connection_loop(self, conn):
        mlog.debug("starting connection loop")
        conn_id = next(self._next_conn_ids)
        info = None

        try:
            req, req_type, req_data = await common.receive_msg(conn)
            if req_type != 'HatEventer.MsgInitReq':
                raise Exception('invalid init request type')

            try:
                info = ConnectionInfo(
                    id=conn_id,
                    client_name=req_data['clientName'],
                    client_token=_optional_from_sbs(req_data['clientToken']),
                    subscription=common.create_subscription(
                        tuple(i) for i in req_data['subscriptions']),
                    server_id=_optional_from_sbs(req_data['serverId']),
                    persisted=req_data['persisted'])

                if self._connected_cb:
                    await aio.call(self._connected_cb, info)

                res_data = 'success', common.status_to_sbs(self._status)
                self._conn_infos[conn] = info

            except Exception as e:
                info = None
                res_data = 'error', str(e)

            mlog.debug("sending init response %s", res_data[0])
            await common.send_msg(conn, 'HatEventer.MsgInitRes', res_data,
                                  conv=req.conv)

            if res_data[0] != 'success':
                with contextlib.suppress(asyncio.TimeoutError):
                    await aio.wait_for(conn.wait_closing(),
                                       self._close_timeout)
                return

            while True:
                mlog.debug("waiting for incomming messages")
                msg, msg_type, msg_data = await common.receive_msg(conn)

                if msg_type == 'HatEventer.MsgStatusAck':
                    mlog.debug("received status ack")
                    future = self._conn_conv_futures.get((conn, msg.conv))
                    if future and not future.done():
                        future.set_result(None)

                elif msg_type == 'HatEventer.MsgRegisterReq':
                    mlog.debug("received register request")
                    await self._process_msg_register(conn, info, msg, msg_data)

                elif msg_type == 'HatEventer.MsgQueryReq':
                    mlog.debug("received query request")
                    await self._process_msg_query(conn, info, msg, msg_data)

                else:
                    raise Exception('unsupported message type')

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("on connection error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping connection loop")
            conn.close()
            self._conn_infos.pop(conn, None)

            for future in self._conn_conv_futures.values():
                if not future.done():
                    future.set_exception(ConnectionError())

            if self._disconnected_cb and info:
                with contextlib.suppress(Exception):
                    await aio.call(self._disconnected_cb, info)

    async def _process_msg_register(self, conn, info, req, req_data):
        register_events = [common.register_event_from_sbs(i)
                           for i in req_data]

        if self._register_cb:
            events = await aio.call(self._register_cb, info, register_events)

        else:
            events = None

        if req.last:
            return

        if events is not None:
            res_data = 'events', [common.event_to_sbs(event)
                                  for event in events]

        else:
            res_data = 'failure', None

        await common.send_msg(conn, 'HatEventer.MsgRegisterRes', res_data,
                              conv=req.conv)

    async def _process_msg_query(self, conn, info, req, req_data):
        params = common.query_params_from_sbs(req_data)

        if self._query_cb:
            result = await aio.call(self._query_cb, info, params)

        else:
            result = common.QueryResult(events=[],
                                        more_follows=False)

        res_data = common.query_result_to_sbs(result)
        await common.send_msg(conn, 'HatEventer.MsgQueryRes', res_data,
                              conv=req.conv)

    async def _notify_status(self, conn, status):
        try:
            req_data = common.status_to_sbs(self._status)
            conv = await common.send_msg(conn, 'HatEventer.MsgStatusNotify',
                                         req_data)

            future = self._loop.create_future()
            self._conn_conv_futures[(conn, conv)] = future

            try:
                await future

            finally:
                self._conn_conv_futures.pop((conn, conv))

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("notify status error: %s", e, exc_info=e)

    async def _notify_events(self, conn, events):
        try:
            msg_data = [common.event_to_sbs(event) for event in events]
            await common.send_msg(conn, 'HatEventer.MsgEventsNotify', msg_data)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("notify events error: %s", e, exc_info=e)


def _optional_from_sbs(data):
    return data[1] if data[0] == 'value' else None
