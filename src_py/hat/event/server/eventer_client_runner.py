from collections.abc import Callable
import asyncio
import functools
import logging
import types
import typing

from hat import aio
from hat import json
from hat import util
from hat.drivers import tcp
import hat.monitor.component

from hat.event import common
from hat.event.server.eventer_client import (SyncedState,
                                             create_eventer_client)


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


SyncedCb: typing.TypeAlias = aio.AsyncCallable[
    [common.ServerId, SyncedState, int | None],
    None]


class EventerServerData(typing.NamedTuple):
    server_id: common.ServerId
    addr: tcp.Address


class EventerClientRunner(aio.Resource):

    def __init__(self,
                 conf: json.Data,
                 backend: common.Backend,
                 synced_cb: SyncedCb,
                 reconnect_delay: float = 5):
        self._conf = conf
        self._backend = backend
        self._synced_cb = synced_cb
        self._reconnect_delay = reconnect_delay
        self._async_group = aio.Group()
        self._operational = False
        self._operational_cbs = util.CallbackRegistry()
        self._valid_server_data = set()
        self._active_server_data = set()
        self._operational_server_data = set()

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    @property
    def operational(self) -> bool:
        return self._operational

    def register_operational_cb(self,
                                cb: Callable[[bool], None]
                                ) -> util.RegisterCallbackHandle:
        return self._operational_cbs.register(cb)

    def set_monitor_state(self, state: hat.monitor.component.State):
        self._valid_server_data = set(_get_eventer_server_data(
            group=self._conf['monitor_component']['group'],
            server_token=self._conf.get('server_token'),
            state=state))

        for server_data in self._valid_server_data:
            if server_data in self._active_server_data:
                continue

            self.async_group.spawn(self._client_loop, server_data)
            self._active_server_data.add(server_data)

    async def _client_loop(self, server_data):
        try:
            mlog.debug("staring eventer client runner loop")
            while server_data in self._valid_server_data:
                self._set_client_status(server_data, None)

                try:
                    mlog.debug("creating eventer client")
                    eventer_client = await create_eventer_client(
                        addr=server_data.addr,
                        client_name=f"event/{self._conf['name']}",
                        local_server_id=self._conf['server_id'],
                        remote_server_id=server_data.server_id,
                        backend=self._backend,
                        client_token=self._conf.get('server_token'),
                        status_cb=functools.partial(self._set_client_status,
                                                    server_data.server_id),
                        synced_cb=functools.partial(self._synced_cb,
                                                    server_data.server_id))

                except Exception:
                    await asyncio.sleep(self._reconnect_delay)
                    continue

                self._set_client_status(server_data, eventer_client.status)

                try:
                    await eventer_client.wait_closing()

                finally:
                    await aio.uncancellable(eventer_client.async_close())

        except Exception as e:
            mlog.error("eventer client runner loop error: %s", e, exc_info=e)
            self.close()

        finally:
            mlog.debug("closing eventer client runner loop")
            self._active_server_data.remove(server_data)
            self._set_client_status(server_data, None)

    def _set_client_status(self, server_data, status):
        if status == common.Status.OPERATIONAL:
            self._operational_server_data.add(server_data)

        else:
            self._operational_server_data.discard(server_data)

        operational = bool(self._operational_server_data)
        if operational == self._operational:
            return

        self._operational = operational
        self._operational_cbs.notify(operational)


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
