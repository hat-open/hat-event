"""Syncer server"""

import asyncio
import logging

from hat import aio
from hat import json
from hat import util

from hat.event.server import common
import hat.event.syncer


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


async def create_syncer_server(conf: json.Data,
                               backend: common.Backend
                               ) -> 'SyncerServer':
    """Create syncer server

    Args:
        conf: configuration defined by
            ``hat-event://main.yaml#/definitions/syncer_server``
        backend: backend

    """
    server = SyncerServer()
    server._backend = backend

    subscriptions = ([tuple(i) for i in conf['subscriptions']]
                     if 'subscriptions' in conf
                     else [('*',)])

    server._server = await hat.event.syncer.listen(
        address=conf['address'],
        query_cb=backend.query_flushed,
        subscriptions=subscriptions,
        token=conf.get('token'))

    server.async_group.spawn(server._backend_loop)

    return server


class SyncerServer(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._server.async_group

    @property
    def state(self) -> list[hat.event.syncer.ClientInfo]:
        """State of all active connections"""
        return self._server.state

    def register_state_cb(self,
                          cb: hat.event.syncer.StateCb
                          ) -> util.RegisterCallbackHandle:
        """Register state change callback"""
        return self._server.register_state_cb(cb)

    async def flush(self):
        """Send flush requests and wait for flush responses"""
        await self._server.flush()

    async def _backend_loop(self):
        try:
            with self._backend.register_flushed_events_cb(self._server.notify):
                await asyncio.Future()

        except Exception as e:
            mlog.error("backend loop error: %s", e, exc_info=e)

        finally:
            self.close()
