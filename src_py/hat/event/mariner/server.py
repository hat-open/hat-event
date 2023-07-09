import asyncio
import contextlib
import logging
import typing

from hat import aio
from hat.drivers import tcp

from hat.event.mariner import common
from hat.event.mariner.transport import Transport


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

ServerConnectionCb: typing.TypeAlias = aio.AsyncCallable[['ServerConnection'],
                                                         None]
"""Server connection callback"""


async def listen(address: tcp.Address,
                 connection_cb: ServerConnectionCb,
                 ping_delay: int = 30,
                 ping_timeout: int = 10,
                 subscriptions: list[common.EventType] = [('*')],
                 *,
                 bind_connections: bool = True,
                 **kwargs
                 ) -> 'Server':
    """Create listening server

    Additional arguments are passed directly to `hat.drivers.tcp.listen`.

    """
    server = Server()
    server._connection_cb = connection_cb
    server._ping_delay = ping_delay
    server._ping_timeout = ping_timeout
    server._subscription = common.Subscription(subscriptions)

    server._server = await tcp.listen(server._on_connection, address,
                                      bind_connections=bind_connections,
                                      **kwargs)

    mlog.debug('listening on %s', address)
    return server


class Server(aio.Resource):
    """Mariner server"""

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._server.async_group

    async def _on_connection(self, conn):
        try:
            transport = Transport(conn)

            msg = await transport.receive()

            if not isinstance(msg, common.InitMsg):
                raise Exception('invalid initialization')

            subscription = self._subscription.intersection(
                common.Subscription(msg.subscriptions))

            srv_conn = ServerConnection()
            srv_conn._transport = transport
            srv_conn._ping_delay = self._ping_delay
            srv_conn._ping_timeout = self._ping_timeout
            srv_conn._client_id = msg.client_id
            srv_conn._client_token = msg.client_token
            srv_conn._last_event_id = msg.last_event_id
            srv_conn._subscription = subscription
            srv_conn._ping_event = asyncio.Event()

            srv_conn.async_group.spawn(srv_conn._receive_loop)
            srv_conn.async_group.spawn(srv_conn._ping_loop)

            await aio.call(self._connection_cb, srv_conn)

        except Exception as e:
            mlog.error("on connection error: %s", e, exc_info=e)
            conn.close()


class ServerConnection(aio.Resource):
    """Mariner server connection"""

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._transport.async_group

    @property
    def client_id(self) -> str:
        """Client id"""
        return self._client_id

    @property
    def client_token(self) -> str | None:
        """Client token"""
        return self._client_token

    @property
    def last_event_id(self) -> common.EventId | None:
        """Laste event id"""
        return self._last_event_id

    @property
    def subscription(self) -> common.Subscription:
        """Subscription"""
        return self._subscription

    async def send_events(self, events: list[common.Event]):
        """Send events"""
        await self._transport.send(common.EventsMsg(events=events))

    async def _receive_loop(self):
        try:
            mlog.debug("starting receive loop")

            while True:
                msg = await self._transport.receive()
                self._ping_event.set()

                if isinstance(msg, common.PingMsg):
                    await self._transport.send(common.PongMsg())

                elif isinstance(msg, common.PongMsg):
                    pass

                else:
                    raise Exception("unsupported msg: %s", msg)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("receive loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping receive loop")
            self.close()

    async def _ping_loop(self):
        try:
            mlog.debug("starting ping loop")

            while True:
                self._ping_event.clear()

                with contextlib.suppress(asyncio.TimeoutError):
                    await aio.wait_for(self._ping_event.wait(),
                                       self._ping_delay)
                    continue

                await self._transport.send(common.PingMsg())

                await aio.wait_for(self._ping_event.wait(),
                                   self._ping_timeout)

        except ConnectionError:
            pass

        except asyncio.TimeoutError:
            mlog.debug("ping timeout")

        except Exception as e:
            mlog.error("ping loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping ping loop")
            self.close()
