from pathlib import Path
import argparse
import typing

from hat import json

from hat.event.server.backends.lmdb.manager import common


def create_argument_parser(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser('copy')
    parser.add_argument('--skip-latest', action='store_true')
    parser.add_argument('--skip-ordered', action='store_true')
    parser.add_argument('src_path', type=Path)
    parser.add_argument('dst_path', type=Path)
    return parser


def copy(args) -> typing.Iterable[json.Data]:
    _act_copy(args.src_path, args.dst_path, args.skip_latest,
              args.skip_ordered)
    yield from []


def _act_copy(src_path, dst_path, skip_latest, skip_ordered):
    db_names = [common.DbType.SYSTEM,
                common.DbType.REF,
                *([common.DbType.LATEST_DATA,
                   common.DbType.LATEST_TYPE]
                  if not skip_latest else []),
                *([common.DbType.ORDERED_DATA,
                   common.DbType.ORDERED_PARTITION,
                   common.DbType.ORDERED_COUNT]
                  if not skip_latest else [])]

    with common.ext_create_env(src_path, readonly=True) as src_env:
        with common.ext_create_env(dst_path) as dst_env:
            with src_env.begin(buffers=True) as src_txn:
                for db_name in db_names:
                    src_db = common.ext_open_db(src_env, db_name)
                    dst_db = common.ext_open_db(dst_env, db_name)

                    with dst_env.begin(db=dst_db, write=True) as dst_txn:
                        for key, value in src_txn.cursor(db=src_db):
                            dst_txn.put(key, value)
