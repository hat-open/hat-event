import logging
import typing

from hat import aio
from hat.drivers import tcp

from hat.event.mariner import common
from hat.event.mariner.transport import Transport


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

EventsCb = aio.AsyncCallable[[typing.List[common.Event]], None]


async def connect(address: tcp.Address,
                  client_id: str,
                  client_token: typing.Optional[str] = None,
                  last_event_id: typing.Optional[common.EventId] = None,
                  subscriptions: typing.List[common.EventType] = [],
                  events_cb: typing.Optional[EventsCb] = None,
                  **kwargs
                  ) -> 'Client':
    """Connect to mariner server"""
    client = Client()
    client._events_cb = events_cb

    conn = await tcp.connect(address, **kwargs)

    try:
        client._transport = Transport(conn)

    except BaseException:
        await aio.uncancellable(conn.async_close())
        raise

    return client


class Client(aio.Resource):

    @property
    def async_group(self):
        return self._transport.async_group
