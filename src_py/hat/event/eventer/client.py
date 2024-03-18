"""Eventer Client"""

from collections.abc import Collection, Iterable
import asyncio
import logging
import typing

from hat import aio
from hat.drivers import chatter
from hat.drivers import tcp

from hat.event.eventer import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

StatusCb: typing.TypeAlias = aio.AsyncCallable[['Client', common.Status],
                                               None]
"""Status callback"""

EventsCb: typing.TypeAlias = aio.AsyncCallable[['Client',
                                                Collection[common.Event]],
                                               None]
"""Events callback"""


class EventerInitError(Exception):
    """Eventer initialization error"""


async def connect(addr: tcp.Address,
                  client_name: str,
                  *,
                  client_token: str | None = None,
                  subscriptions: Iterable[common.EventType] = [],
                  server_id: common.ServerId | None = None,
                  persisted: bool = False,
                  status_cb: StatusCb | None = None,
                  events_cb: EventsCb | None = None,
                  **kwargs
                  ) -> 'Client':
    """Connect to Eventer Server

    Arguments `client_name` and optional `client_token` identifies eventer
    client.

    According to Event Server specification, each subscription is event
    type identifier which can contain special subtypes ``?`` and ``*``.
    Subtype ``?`` can occur at any position inside event type identifier
    and is used as replacement for any single subtype. Subtype ``*`` is valid
    only as last subtype in event type identifier and is used as replacement
    for zero or more arbitrary subtypes.

    If `subscriptions` is empty list, client doesn't subscribe for any events
    and will not receive server's notifications.

    If `server_id` is ``None``, client will receive all event notifications,
    in accordance to `subscriptions`, regardless of event's server id. If
    `server_id` is set, Eventer Server will only send events notifications
    for events with provided server id.

    If `persisted` is set to ``True``, Eventer Server will notify events
    after they are persisted (flushed to disk). Otherwise, events are
    notified immediately after registration.

    Additional arguments are passed to `hat.chatter.connect` coroutine.

    """
    client = Client()
    client._status_cb = status_cb
    client._events_cb = events_cb
    client._loop = asyncio.get_running_loop()
    client._conv_futures = {}
    client._status = common.Status.STANDBY

    client._conn = await chatter.connect(addr, **kwargs)

    try:
        req_data = {'clientName': client_name,
                    'clientToken': _optional_to_sbs(client_token),
                    'subscriptions': [list(i) for i in subscriptions],
                    'serverId': _optional_to_sbs(server_id),
                    'persisted': persisted}
        conv = await common.send_msg(conn=client._conn,
                                     msg_type='HatEventer.MsgInitReq',
                                     msg_data=req_data,
                                     last=False)

        res, res_type, res_data = await common.receive_msg(client._conn)
        if res_type != 'HatEventer.MsgInitRes' or res.conv != conv:
            raise Exception('invalid init response')

        if res_data[0] == 'success':
            client._status = common.Status(common.status_from_sbs(res_data[1]))

        elif res_data[0] == 'error':
            raise EventerInitError(res_data[1])

        else:
            raise ValueError('unsupported init response')

        client.async_group.spawn(client._receive_loop)

    except BaseException:
        await aio.uncancellable(client.async_close())
        raise

    return client


class Client(aio.Resource):
    """Eventer client

    For creating new client see `connect` coroutine.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._conn.async_group

    @property
    def status(self) -> common.Status:
        """Status"""
        return self._status

    async def register(self,
                       events: Collection[common.RegisterEvent],
                       with_response: bool = False
                       ) -> Collection[common.Event] | None:
        """Register events and optionally wait for response

        If `with_response` is ``True``, this coroutine returns list of events
        or ``None`` if registration failure occurred.

        """
        msg_data = [common.register_event_to_sbs(i) for i in events]
        conv = await common.send_msg(conn=self._conn,
                                     msg_type='HatEventer.MsgRegisterReq',
                                     msg_data=msg_data,
                                     last=not with_response)

        if with_response:
            return await self._wait_conv_res(conv)

    async def query(self,
                    params: common.QueryParams
                    ) -> common.QueryResult:
        """Query events from server"""
        msg_data = common.query_params_to_sbs(params)
        conv = await common.send_msg(conn=self._conn,
                                     msg_type='HatEventer.MsgQueryReq',
                                     msg_data=msg_data,
                                     last=False)

        return await self._wait_conv_res(conv)

    async def _receive_loop(self):
        mlog.debug("starting receive loop")
        try:
            while True:
                mlog.debug("waiting for incoming message")
                msg, msg_type, msg_data = await common.receive_msg(self._conn)

                if msg_type == 'HatEventer.MsgStatusNotify':
                    mlog.debug("received status notification")
                    await self._process_msg_status_notify(msg, msg_data)

                elif msg_type == 'HatEventer.MsgEventsNotify':
                    mlog.debug("received events notification")
                    await self._process_msg_events_notify(msg, msg_data)

                elif msg_type == 'HatEventer.MsgRegisterRes':
                    mlog.debug("received register response")
                    await self._process_msg_register_res(msg, msg_data)

                elif msg_type == 'HatEventer.MsgQueryRes':
                    mlog.debug("received query response")
                    await self._process_msg_query_res(msg, msg_data)

                else:
                    raise Exception("unsupported message type")

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("read loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping receive loop")
            self.close()

            for future in self._conv_futures.values():
                if not future.done():
                    future.set_exception(ConnectionError())

    async def _wait_conv_res(self, conv):
        if not self.is_open:
            raise ConnectionError()

        future = self._loop.create_future()
        self._conv_futures[conv] = future

        try:
            return await future

        finally:
            self._conv_futures.pop(conv, None)

    async def _process_msg_status_notify(self, msg, msg_data):
        self._status = common.status_from_sbs(msg_data)

        if self._status_cb:
            await aio.call(self._status_cb, self, self._status)

        await common.send_msg(conn=self._conn,
                              msg_type='HatEventer.MsgStatusAck',
                              msg_data=None,
                              conv=msg.conv)

    async def _process_msg_events_notify(self, msg, msg_data):
        events = [common.event_from_sbs(event) for event in msg_data]

        if self._events_cb:
            await aio.call(self._events_cb, self, events)

    async def _process_msg_register_res(self, msg, msg_data):
        if msg_data[0] == 'events':
            result = [common.event_from_sbs(event) for event in msg_data[1]]

        elif msg_data[0] == 'failure':
            result = None

        else:
            raise ValueError('unsupported register response')

        future = self._conv_futures.get(msg.conv)
        if not future or future.done():
            return

        future.set_result(result)

    async def _process_msg_query_res(self, msg, msg_data):
        result = common.query_result_from_sbs(msg_data)

        future = self._conv_futures.get(msg.conv)
        if not future or future.done():
            return

        future.set_result(result)


def _optional_to_sbs(value):
    return ('value', value) if value is not None else ('none', None)
