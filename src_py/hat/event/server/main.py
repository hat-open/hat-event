"""Event server main"""

from pathlib import Path
import argparse
import asyncio
import contextlib
import importlib
import logging.config
import sys

import appdirs

from hat import aio
from hat import json

from hat.event.server import common
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
        help="configuration defined by hat-event://server.yaml# "
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

    common.json_schema_repo.validate('hat-event://server.yaml#', conf)

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
