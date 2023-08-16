import asyncio
import enum
import functools
import logging
import typing

from hat import aio
from hat import util
import hat.monitor.client

from hat.event.server import common
import hat.event.syncer


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

reconnect_delay: float = 10
"""Reconnect delay"""


class SyncerClientState(enum.Enum):
    """Connection state"""
    CONNECTED = 0
    SYNCED = 1
    DISCONNECTED = 2


ServerId: typing.TypeAlias = int
"""Server identifier"""

StateCb: typing.TypeAlias = typing.Callable[[ServerId, SyncerClientState],
                                            None]
"""Connection state callback"""

EventsCb: typing.TypeAlias = typing.Callable[[ServerId, list[common.Event]],
                                             None]
"""Events callback"""


async def create_syncer_client(backend: common.Backend,
                               monitor_client: hat.monitor.client.Client,
                               monitor_group: str,
                               name: str,
                               syncer_token: str | None = None,
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
    cli._conns = {}
    cli._state_cbs = util.CallbackRegistry()
    cli._events_cbs = util.CallbackRegistry()
    cli._async_group = aio.Group()

    cli.async_group.spawn(cli._monitor_client_loop)

    return cli


class SyncerClient(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._async_group

    def register_state_cb(self,
                          cb: StateCb
                          ) -> util.RegisterCallbackHandle:
        """Register client state callback"""
        return self._state_cbs.register(cb)

    def register_events_cb(self,
                           cb: EventsCb
                           ) -> util.RegisterCallbackHandle:
        """Register events callback"""
        return self._events_cbs.register(cb)

    async def _monitor_client_loop(self):
        try:
            changes = aio.Queue()
            change_cb = functools.partial(changes.put_nowait, None)
            with self._monitor_client.register_change_cb(change_cb):

                while True:
                    mlog.debug("filtering syncer server addresses")
                    server_id_addresses = {}
                    for info in self._monitor_client.components:
                        if not (info.group == self._monitor_group and
                                info != self._monitor_client.info and
                                info.data and
                                'server_id' in info.data and
                                'syncer_server_address' in info.data):
                            continue
                        if (self._syncer_token is not None and
                                self._syncer_token != info.data.get('syncer_token')):  # NOQA
                            mlog.warning("syncer tokens not equal, server %s "
                                         "ignored for syncer connection to %s",
                                         info.data['server_id'],
                                         info.data['syncer_server_address'])
                            continue
                        server_id_addresses[info.data['server_id']] = \
                            info.data['syncer_server_address']

                    for server_id, address in server_id_addresses.items():
                        conn = self._conns.get(server_id)
                        if conn:
                            if conn.is_open and conn.address == address:
                                continue

                            mlog.debug("closing existing connection")
                            await conn.async_close()
                            self._conns.pop(server_id)

                        mlog.debug("creating new connection")
                        state_cb = functools.partial(self._state_cbs.notify,
                                                     server_id)
                        events_cb = functools.partial(self._events_cbs.notify,
                                                      server_id)
                        conn = _Connection(
                            async_group=self.async_group.create_subgroup(),
                            backend=self._backend,
                            server_id=server_id,
                            address=address,
                            client_name=self._name,
                            kwargs=self._kwargs,
                            state_cb=state_cb,
                            events_cb=events_cb)

                        self._conns[server_id] = conn

                    await changes.get_until_empty()

        except Exception as e:
            mlog.error("monitor client loop error: %s", e, exc_info=e)

        finally:
            self.close()


class _Connection(aio.Resource):

    def __init__(self, async_group, backend, server_id, address, client_name,
                 kwargs, state_cb, events_cb):
        self._async_group = async_group
        self._backend = backend
        self._server_id = server_id
        self._address = address
        self._client_name = client_name
        self._kwargs = kwargs
        self._state_cb = state_cb
        self._events_cb = events_cb
        self._synced = False

        self.async_group.spawn(self._connection_loop)

    @property
    def async_group(self):
        return self._async_group

    @property
    def address(self):
        return self._address

    @property
    def synced(self):
        return self._synced

    def _on_synced(self):
        self._synced = True
        self._state_cb(SyncerClientState.SYNCED)

    async def _connection_loop(self):
        while True:
            try:
                mlog.debug("querying last event id")
                last_event_id = await self._backend.get_last_event_id(
                    self._server_id)

                mlog.debug("connecting to syncer server")
                client = await hat.event.syncer.connect(
                    address=self._address,
                    client_name=self._client_name,
                    last_event_id=last_event_id,
                    synced_cb=self._on_synced,
                    events_cb=self._backend.register)

            except Exception as e:
                mlog.debug("can not connect to syncer server: %s", e)
                await asyncio.sleep(reconnect_delay)
                continue

            try:
                self._synced = False
                self._state_cb(SyncerClientState.CONNECTED)

                await client.wait_closing()

            except Exception as e:
                mlog.error("connection loop error: %s", e, exc_info=e)

            finally:
                try:
                    await aio.uncancellable(client.async_close())

                finally:
                    self._synced = False
                    self._state_cb(SyncerClientState.DISCONNECTED)
