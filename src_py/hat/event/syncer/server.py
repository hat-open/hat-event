import typing

from hat import aio
from hat import util

from hat.event.syncer import common


class ClientInfo(typing.NamedTuple):
    """Client connection information"""
    name: str
    synced: bool


StateCb = typing.Callable[[typing.List[ClientInfo]], None]
"""Syncer state change callback"""

QueryCb = typing.Callable[[common.EventId],
                          typing.AsyncIterable[typing.List[common.Event]]]


async def listen(address: str,
                 syncer_token: typing.Optional[str] = None,
                 query_cb: typing.Optional[QueryCb] = None
                 ) -> 'Server':
    pass


class Server(aio.Resource):

    @property
    def async_group(self):
        return self._srv.async_group

    @property
    def state(self) -> typing.Iterable[ClientInfo]:
        """State of all active connections"""
        return self._state.values()

    def register_state_cb(self,
                          cb: StateCb
                          ) -> util.RegisterCallbackHandle:
        """Register state change callback"""
        return self._state_cbs.register(cb)

    def notify(self, events: typing.List[common.Event]):
        pass
