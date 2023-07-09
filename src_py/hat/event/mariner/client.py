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

EventsCb: typing.TypeAlias = aio.AsyncCallable[[list[common.Event]], None]
"""Events callback"""


async def connect(address: tcp.Address,
                  client_id: str,
                  client_token: str | None = None,
                  last_event_id: common.EventId | None = None,
                  subscriptions: list[common.EventType] = [],
                  events_cb: EventsCb | None = None,
                  ping_delay: int = 30,
                  ping_timeout: int = 10,
                  **kwargs
                  ) -> 'Client':
    """Connect to mariner server

    Additional arguments are passed directly to `hat.drivers.tcp.connect`.

    """
    conn = await tcp.connect(address, **kwargs)

    try:
        transport = Transport(conn)

        msg = common.InitMsg(client_id=client_id,
                             client_token=client_token,
                             last_event_id=last_event_id,
                             subscriptions=subscriptions)
        await transport.send(msg)

        return Client(transport, events_cb, ping_delay, ping_timeout)

    except BaseException:
        await aio.uncancellable(conn.async_close())
        raise


class Client(aio.Resource):
    """Mariner client

    For creation of new instance see `connect` coroutine.

    """

    def __init__(self,
                 transport: Transport,
                 events_cb: EventsCb | None,
                 ping_delay: int,
                 ping_timeout: int):
        self._transport = transport
        self._events_cb = events_cb
        self._ping_delay = ping_delay
        self._ping_timeout = ping_timeout
        self._ping_event = asyncio.Event()

        self.async_group.spawn(self._receive_loop)
        self.async_group.spawn(self._ping_loop)

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._transport.async_group

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

                elif isinstance(msg, common.EventsMsg):
                    if self._events_cb:
                        await aio.call(self._events_cb, msg.events)

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
            mlog.debug("starting ping loop %s", id(self))

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
