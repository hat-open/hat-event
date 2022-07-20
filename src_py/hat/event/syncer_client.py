import asyncio
import logging
import typing

from hat import aio
from hat import chatter
from hat.event.server import common
import hat.event.common.data
import hat.monitor.client


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


reconnect_delay: float = 10
"""Reconnect delay"""


async def create_syncer_client(backend: common.Backend,
                               monitor_client: hat.monitor.client.Client,
                               monitor_group: str,
                               name: str,
                               syncer_token: typing.Optional[str] = None,
                               **kwargs
                               ) -> 'SyncerClient':
    """Create syncer client

    Args:
        backend: backend
        monitor_client: monitor client
        monitor_group: monitor group name
        name: client name
        syncer_token: syncer token
        kwargs: additional arguments passed to `hat.chatter.connect` coroutine

    """
    cli = SyncerClient()
    cli._backend = backend
    cli._monitor_client = monitor_client
    cli._monitor_group = monitor_group
    cli._name = name
    cli._syncer_token = syncer_token
    cli._kwargs = kwargs
    cli._syncers = {}
    cli._servers_info_queue = aio.Queue()
    cli._monitor_change_handler = monitor_client.register_change_cb(
        cli._on_monitor_change)
    cli._async_group = monitor_client.async_group.create_subgroup()
    cli._async_group.spawn(aio.call_on_cancel, cli._close)
    return cli


class SyncerClient(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._async_group

    def _close(self):
        for syncer in self._syncers.values():
            syncer.close()
        self._monitor_change_handler.cancel()

    def _on_monitor_change(self):
        mlog.debug("received monitor change")
        servers_info = [
            (info.data['server_id'], info.data['syncer_server_address'])
            for info in self._monitor_client.components
            if (info.group == self._monitor_group
                and info != self._monitor_client.info
                and (self._syncer_token is None
                     or self._syncer_token == info.data['syncer_token']))]

        syncers_to_close = [server_info for server_info in self._syncers.keys()
                            if server_info not in servers_info]
        for server_info in syncers_to_close:
            mlog.debug("stopping synchronization with event server id=%s",
                       server_info[0])
            syncer = self._syncers.pop(server_info)
            syncer.close()

        for server_info in servers_info:
            if server_info in self._syncers:
                continue
            mlog.debug("starting synchronization with event server id=%s",
                       server_info[0])
            syncer_group = self.async_group.create_subgroup()
            syncer_group.spawn(_syncer_loop, self._backend, server_info[0],
                               server_info[1], self._name, self._kwargs)
            self._syncers[server_info] = syncer_group


async def _syncer_loop(backend, server_id, syncer_server_address, client_name,
                       kwargs):
    while True:
        try:
            mlog.debug("connecting to syncer server")
            conn = await chatter.connect(common.sbs_repo,
                                         syncer_server_address, **kwargs)

        except Exception:
            mlog.info("can not connect to syncer server")
            await asyncio.sleep(reconnect_delay)
            continue

        try:
            last_event_id = await backend.get_last_event_id(server_id)
            msg_data = chatter.Data(
                module='HatSyncer',
                type='MsgReq',
                data=hat.event.common.data.syncer_req_to_sbs(
                     hat.event.common.data.SyncerReq(last_event_id,
                                                     client_name)))
            conn.send(msg_data)

            while True:
                mlog.debug("waiting for incoming message")
                msg = await conn.receive()
                msg_type = msg.data.module, msg.data.type

                if msg_type == ('HatSyncer', 'MsgEvents'):
                    mlog.debug("received events")
                    events = [common.event_from_sbs(i) for i in msg.data.data]
                    await backend.register(events)

                elif msg_type == ('HatSyncer', 'MsgSynced'):
                    mlog.info("received synced")

                else:
                    raise Exception("unsupported message type")

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("syncer loop error: %s", e, exc_info=e)

        finally:
            await aio.uncancellable(conn.async_close())
