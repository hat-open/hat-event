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
from hat.event.server.syncer_server import create_syncer_server, SyncerServer
from hat.event.syncer_client import create_syncer_client
import hat.monitor.client
import hat.monitor.common


mlog: logging.Logger = logging.getLogger('hat.event.server.main')
"""Module logger"""

user_conf_dir: Path = Path(appdirs.user_config_dir('hat'))
"""User configuration directory path"""


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
    loop = aio.init_asyncio()

    common.json_schema_repo.validate('hat-event://main.yaml#', conf)

    sub_confs = [conf['backend'], *conf['engine']['modules']]
    for sub_conf in sub_confs:
        module = importlib.import_module(sub_conf['module'])
        if module.json_schema_repo and module.json_schema_id:
            module.json_schema_repo.validate(module.json_schema_id, sub_conf)

    logging.config.dictConfig(conf['log'])

    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main(conf), loop=loop)


async def async_main(conf: json.Data):
    """Async main entry point"""
    async_group = aio.Group()
    async_group.spawn(aio.call_on_cancel, asyncio.sleep, 0.1)

    try:
        py_module = importlib.import_module(conf['backend']['module'])
        backend = await aio.call(py_module.create, conf['backend'])
        _bind_resource(async_group, backend)

        async_subgroup = async_group.create_subgroup()

        try:
            syncer_server = await create_syncer_server(conf['syncer_server'],
                                                       backend)
            _bind_resource(async_subgroup, syncer_server)

            if 'monitor' in conf:
                data = {
                    'server_id': conf['server_id'],
                    'eventer_server_address': conf['eventer_server']['address'],  # NOQA
                    'syncer_server_address': conf['syncer_server']['address']}
                if 'syncer_token' in conf:
                    data['syncer_token'] = conf['syncer_token']
                monitor = await hat.monitor.client.connect(conf['monitor'],
                                                           data)
                _bind_resource(async_subgroup, monitor)

                syncer_client = await create_syncer_client(
                    backend, monitor, conf['monitor']['group'])
                _bind_resource(async_subgroup, syncer_client)

                component = hat.monitor.client.Component(
                    monitor, run, conf, backend, syncer_server)
                component.set_ready(True)
                _bind_resource(async_subgroup, component)

                await async_subgroup.wait_closing()

            else:
                await async_subgroup.spawn(run, None, conf, backend,
                                           syncer_server)

        finally:
            await aio.uncancellable(async_subgroup.async_close())

    finally:
        await aio.uncancellable(async_group.async_close())


async def run(component: typing.Optional[hat.monitor.client.Component],
              conf: json.Data,
              backend: common.Backend,
              syncer_server: SyncerServer):
    """Run monitor component"""

    def on_syncer_client_state(source, client_name, state):
        event = common.RegisterEvent(
            event_type=('event', 'syncer', client_name),
            source_timestamp=None,
            payload=common.EventPayload(
                type=common.EventPayloadType.JSON,
                data=state.name))
        async_group.spawn(engine.register, source, [event])

    async_group = aio.Group()

    # TODO wait depending on ???

    try:
        engine = await create_engine(conf['engine'], backend)
        _bind_resource(async_group, engine)

        with syncer_server.register_client_state_cb(on_syncer_client_state):
            eventer_server = await create_eventer_server(
                conf['eventer_server'], engine)
            _bind_resource(async_group, eventer_server)

            await async_group.wait_closing()

    finally:
        await aio.uncancellable(async_group.async_close())


def _bind_resource(async_group, resource):
    async_group.spawn(aio.call_on_cancel, resource.async_close)
    async_group.spawn(aio.call_on_done, resource.wait_closing(),
                      async_group.close)


if __name__ == '__main__':
    sys.argv[0] = 'hat-event'
    sys.exit(main())
