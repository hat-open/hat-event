"""Event server main"""

from pathlib import Path
import argparse
import asyncio
import contextlib
import logging.config
import sys

import appdirs

from hat import aio
from hat import json

from hat.event import common
from hat.event.server.runner import MainRunner


mlog: logging.Logger = logging.getLogger('hat.event.server.main')
"""Module logger"""

user_conf_dir: Path = Path(appdirs.user_config_dir('hat'))
"""User configuration directory path"""


def create_argument_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--conf', metavar='PATH', type=Path, default=None,
        help="configuration defined by hat-event://server.yaml "
             "(default $XDG_CONFIG_HOME/hat/event.{yaml|yml|toml|json})")
    return parser


def main():
    """Event Server"""
    parser = create_argument_parser()
    args = parser.parse_args()
    conf = json.read_conf(args.conf, user_conf_dir / 'event')
    sync_main(conf)


def sync_main(conf: json.Data):
    """Sync main entry point"""
    aio.init_asyncio()

    common.json_schema_repo.validate('hat-event://server.yaml', conf)

    info = common.import_backend_info(conf['backend']['module'])
    if info.json_schema_repo and info.json_schema_id:
        info.json_schema_repo.validate(info.json_schema_id, conf['backend'])

    for module_conf in conf['modules']:
        info = common.import_module_info(module_conf['module'])
        if info.json_schema_repo and info.json_schema_id:
            info.json_schema_repo.validate(info.json_schema_id, module_conf)

    log_conf = conf.get('log')
    if log_conf:
        logging.config.dictConfig(log_conf)

    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main(conf))


async def async_main(conf: json.Data):
    """Async main entry point"""
    main_runner = MainRunner(conf)

    async def cleanup():
        await main_runner.async_close()
        await asyncio.sleep(0.1)

    try:
        await main_runner.wait_closing()

    finally:
        await aio.uncancellable(cleanup())


if __name__ == '__main__':
    sys.argv[0] = 'hat-event-server'
    sys.exit(main())
