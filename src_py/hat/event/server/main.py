"""Event server main"""

from pathlib import Path
import argparse
import asyncio
import contextlib
import importlib
import logging.config
import sys
import typing

import appdirs

from hat import aio
from hat import json
from hat.event.server import common
from hat.event.server.engine import create_engine
from hat.event.server.eventer_server import create_eventer_server
from hat.event.server.syncer_server import (create_syncer_server,
                                            SyncerServer)
from hat.event.syncer_client import (create_syncer_client,
                                     SyncerClientState,
                                     SyncerClient)
import hat.monitor.client
import hat.monitor.common


mlog: logging.Logger = logging.getLogger('hat.event.server.main')
"""Module logger"""

user_conf_dir: Path = Path(appdirs.user_config_dir('hat'))
"""User configuration directory path"""

servers_engine_stopped_timeout: int = 3
"""Timeout for waiting engine to be stopped on all servers syncer client is
connected to"""


def create_argument_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--conf', metavar='PATH', type=Path, default=None,
        help="configuration defined by hat-event://main.yaml# "
             "(default $XDG_CONFIG_HOME/hat/event.{yaml|yml|json})")
    return parser


def main():
    """Event Server"""
    parser = create_argument_parser()
    args = parser.parse_args()

    conf_path = args.conf
    if not conf_path:
        for suffix in ('.yaml', '.yml', '.json'):
            conf_path = (user_conf_dir / 'event').with_suffix(suffix)
            if conf_path.exists():
                break

    if conf_path == Path('-'):
        conf = json.decode_stream(sys.stdin)
    else:
        conf = json.decode_file(conf_path)

    sync_main(conf)


def sync_main(conf: json.Data):
    """Sync main entry point"""
    aio.init_asyncio()

    common.json_schema_repo.validate('hat-event://main.yaml#', conf)

    sub_confs = [conf['backend'], *conf['engine']['modules']]
    for sub_conf in sub_confs:
        module = importlib.import_module(sub_conf['module'])
        if module.json_schema_repo and module.json_schema_id:
            module.json_schema_repo.validate(module.json_schema_id, sub_conf)

    logging.config.dictConfig(conf['log'])

    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main(conf))


async def async_main(conf: json.Data):
    """Async main entry point"""
    try:
        await run_backend(conf)

    finally:
        await asyncio.sleep(0.1)


async def run_backend(conf: json.Data):
    py_module = importlib.import_module(conf['backend']['module'])
    backend = await aio.call(py_module.create, conf['backend'])

    async_group = aio.Group(log_exceptions=False)
    async_group.spawn(aio.call_on_done, backend.wait_closing(),
                      async_group.close)

    async def cleanup():
        await async_group.async_close()
        await backend.async_close()

    try:
        await async_group.spawn(run_syncer_server, conf, backend)

    finally:
        await aio.uncancellable(cleanup())


async def run_syncer_server(conf: json.Data,
                            backend: common.Backend):
    syncer_server = await create_syncer_server(conf['syncer_server'],
                                               backend)

    async_group = aio.Group(log_exceptions=False)
    async_group.spawn(aio.call_on_done, syncer_server.wait_closing(),
                      async_group.close)

    async def cleanup():
        await async_group.async_close()
        with contextlib.suppress(Exception):
            await backend.flush()
        await syncer_server.async_close()

    try:
        if 'monitor' in conf:
            await async_group.spawn(run_monitor_client, conf, backend,
                                    syncer_server)
        else:
            await async_group.spawn(run_engine, None, conf, backend,
                                    syncer_server, None)

    finally:
        await aio.uncancellable(cleanup())


async def run_monitor_client(conf: json.Data,
                             backend: common.Backend,
                             syncer_server: SyncerServer):
    async_group = aio.Group()
    monitor = None
    syncer_client = None
    component = None

    async def cleanup():
        if component:
            await component.async_close()
        if syncer_client:
            await syncer_client.async_close()
        if monitor:
            await monitor.async_close()
        await async_group.async_close()

    try:
        data = {'server_id': conf['engine']['server_id'],
                'eventer_server_address': conf['eventer_server']['address'],
                'syncer_server_address': conf['syncer_server']['address']}
        if 'syncer_token' in conf:
            data['syncer_token'] = conf['syncer_token']
        monitor = await hat.monitor.client.connect(conf['monitor'], data)
        async_group.spawn(aio.call_on_done, monitor.wait_closing(),
                          async_group.close)

        syncer_client = await create_syncer_client(
            backend=backend,
            monitor_client=monitor,
            monitor_group=conf['monitor']['group'],
            name=str(conf['engine']['server_id']),
            syncer_token=conf.get('syncer_token'))
        async_group.spawn(aio.call_on_done, syncer_client.wait_closing(),
                          async_group.close)

        component = hat.monitor.client.Component(
            monitor, run_engine, conf, backend, syncer_server, syncer_client)
        component.set_ready(True)
        async_group.spawn(aio.call_on_done, component.wait_closing(),
                          async_group.close)

        await async_group.wait_closing()

    finally:
        await aio.uncancellable(cleanup())


async def run_engine(component: typing.Optional[hat.monitor.client.Component],
                     conf: json.Data,
                     backend: common.Backend,
                     syncer_server: SyncerServer,
                     syncer_client: typing.Optional[SyncerClient]):
    async_group = aio.Group()
    syncer_client_states = {}
    syncer_source = common.Source(type=common.SourceType.SYNCER,
                                  id=0)
    engine = None
    eventer_server = None

    async def cleanup():
        if eventer_server:
            await eventer_server.async_close()
        if engine:
            await engine.async_close()
        with contextlib.suppress(Exception):
            await backend.flush()
        with contextlib.suppress(Exception):
            await syncer_server.flush()
        await async_group.async_close()

    def on_syncer_server_state(client_infos):
        event = common.RegisterEvent(
            event_type=('event', 'syncer', 'server'),
            source_timestamp=None,
            payload=common.EventPayload(
                type=common.EventPayloadType.JSON,
                data=[{'client_name': client_info.name,
                       'synced': client_info.synced}
                      for client_info in client_infos]))
        async_group.spawn(engine.register, syncer_source, [event])

    def on_syncer_client_state(server_id, state):
        syncer_client_states[server_id] = state
        if state != SyncerClientState.SYNCED:
            return

        event = common.RegisterEvent(
            event_type=('event', 'syncer', 'client', str(server_id), 'synced'),
            source_timestamp=None,
            payload=common.EventPayload(
                type=common.EventPayloadType.JSON,
                data=True))
        async_group.spawn(engine.register, syncer_source, [event])

    def on_syncer_client_events(server_id, events):
        state = syncer_client_states.pop(server_id, None)
        if state != SyncerClientState.CONNECTED:
            return

        event = common.RegisterEvent(
            event_type=('event', 'syncer', 'client', str(server_id), 'synced'),
            source_timestamp=None,
            payload=common.EventPayload(
                type=common.EventPayloadType.JSON,
                data=False))
        async_group.spawn(engine.register, syncer_source, [event])

    if syncer_client and syncer_client.servers_synced:
        await _wait_servers_engine_stopped(backend, syncer_client)

    # TODO wait depending on ???

    try:
        engine = await create_engine(conf['engine'], backend)
        async_group.spawn(aio.call_on_done, engine.wait_closing(),
                          async_group.close)

        with contextlib.ExitStack() as exit_stack:
            exit_stack.enter_context(
                syncer_server.register_state_cb(on_syncer_server_state))
            on_syncer_server_state(list(syncer_server.state))

            if syncer_client:
                exit_stack.enter_context(
                    syncer_client.register_state_cb(on_syncer_client_state))
                exit_stack.enter_context(
                    syncer_client.register_events_cb(on_syncer_client_events))

            eventer_server = await create_eventer_server(
                conf['eventer_server'], engine)
            async_group.spawn(aio.call_on_done, eventer_server.wait_closing(),
                              async_group.close)

            await async_group.wait_closing()

    finally:
        await aio.uncancellable(cleanup())


async def _wait_servers_engine_stopped(backend, syncer_client):

    events_queue = aio.Queue()
    engines_running = set()

    async def wait_engines_stopped():
        while engines_running:
            server_id, events = await events_queue.get()
            for event in events:
                if event.event_type != ('event', 'engine'):
                    continue
                if event.payload.data != 'STOPPED':
                    continue
                engines_running.discard(server_id)

    with syncer_client.register_events_cb(
            lambda srv_id, evts: events_queue.put_nowait((srv_id, evts))):
        for server_id in syncer_client.servers_synced:
            res = await backend.query(common.QueryData(
                event_types=[('event', 'engine')],
                server_id=server_id,
                unique_type=True,
                max_results=1))
            engine_event = res[0] if res else None
            if engine_event and engine_event.payload.data == 'STOPPED':
                continue
            engines_running.add(server_id)

        mlog.debug("waiting engines stopped on servers %s", engines_running)
        try:
            await asyncio.wait_for(wait_engines_stopped(),
                                   timeout=servers_engine_stopped_timeout)
        except asyncio.TimeoutError:
            mlog.warning("timeout %s exceeded for stopping engines on %s",
                         servers_engine_stopped_timeout, engines_running)


if __name__ == '__main__':
    sys.argv[0] = 'hat-event'
    sys.exit(main())
