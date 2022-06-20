import typing

from hat import aio
from hat import util
from hat import json

from hat.event.server import common


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
        return self._async_group

    def register_client_state_cb(self,
                                 cb: StateCb
                                 ) -> util.RegisterCallbackHandle:
        """Register client state callback"""
