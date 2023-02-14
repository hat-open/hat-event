import typing
import logging

from hat import aio

from hat.event import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

ClientId = int
"""Client identifier"""

ClientCb = aio.AsyncCallable[[ClientId], None]
"""Client connected/disconnected callback"""

RegisterCb = aio.AsyncCallable[[ClientId, typing.List[common.RegisterEvent]],
                               typing.List[common.Event]]
"""Register callback"""

QueryCb = aio.AsyncCallable[[ClientId, typing.List[common.QueryData]],
                            typing.List[common.Event]]
"""Query callback"""


async def listen(address: str,
                 connected_cb: typing.Optional[ClientCb] = None,
                 disconnected_cb: typing.Optional[ClientCb] = None,
                 register_cb: typing.Optional[RegisterCb] = None,
                 query_cb: typing.Optional[QueryCb] = None
                 ) -> 'Server':
    """Create eventer server instance"""


class Server(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""

    def notify(self, events: typing.List[common.Event]):
        """Notify events to subscribed clients"""
