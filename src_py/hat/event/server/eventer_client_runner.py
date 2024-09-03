import asyncio
import functools
import logging
import types
import typing

from hat import aio
from hat import json
from hat.drivers import tcp
import hat.monitor.component

from hat.event import common
from hat.event.server.eventer_client import (SyncedState,
                                             create_eventer_client)


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class EventerServerData(typing.NamedTuple):
    server_id: common.ServerId
    addr: tcp.Address


class EventerClientRunner(aio.Resource):

    def __init__(self,
                 conf: json.Data,
                 backend: common.Backend,
                 synced_cb: aio.AsyncCallable[[common.ServerId,
                                               SyncedState,
                                               int | None],
                                              None],
                 reconnect_delay: float = 5):
        self._conf = conf
        self._backend = backend
        self._synced_cb = synced_cb
        self._reconnect_delay = reconnect_delay
        self._async_group = aio.Group()
        self._valid_server_data = set()
        self._client_subgroups = {}

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    def set_monitor_state(self, state: hat.monitor.component.State):
        self._valid_server_data = set(_get_eventer_server_data(
            group=self._conf['monitor_component']['group'],
            server_token=self._conf.get('server_token'),
            state=state))

        for server_data in list(self._client_subgroups.keys()):
            if server_data in self._valid_server_data:
                continue

            subgroup = self._client_subgroups.pop(server_data)
            subgroup.close()

        for server_data in self._valid_server_data:
            subgroup = self._client_subgroups.get(server_data)
            if subgroup and subgroup.is_open:
                continue

            subgroup = self.async_group.create_subgroup()
            subgroup.spawn(self._client_loop, subgroup, server_data)
            self._client_subgroups[server_data] = subgroup

    async def _client_loop(self, async_group, server_data):
        try:
            mlog.debug("staring eventer client runner loop")
            while True:
                try:
                    mlog.debug("creating eventer client")
                    eventer_client = await create_eventer_client(
                        addr=server_data.addr,
                        client_name=f"event/{self._conf['name']}",
                        local_server_id=self._conf['server_id'],
                        remote_server_id=server_data.server_id,
                        backend=self._backend,
                        client_token=self._conf.get('server_token'),
                        synced_cb=functools.partial(self._synced_cb,
                                                    server_data.server_id))

                except Exception:
                    await asyncio.sleep(self._reconnect_delay)
                    continue

                try:
                    await eventer_client.wait_closing()

                finally:
                    await aio.uncancellable(eventer_client.async_close())

        except Exception as e:
            mlog.error("eventer client runner loop error: %s", e, exc_info=e)
            self.close()

        finally:
            mlog.debug("closing eventer client runner loop")
            async_group.close()


def _get_eventer_server_data(group, server_token, state):
    for info in state.components:
        if info == state.info or info.group != group:
            continue

        server_id = json.get(info.data, 'server_id')
        host = json.get(info.data, ['eventer_server', 'host'])
        port = json.get(info.data, ['eventer_server', 'port'])
        token = json.get(info.data, 'server_token')
        if (not isinstance(server_id, int) or
                not isinstance(host, str) or
                not isinstance(port, int) or
                not isinstance(token, (str, types.NoneType))):
            continue

        if server_token is not None and token != server_token:
            continue

        yield EventerServerData(server_id=server_id,
                                addr=tcp.Address(host, port))
