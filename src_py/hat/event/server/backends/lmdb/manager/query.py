from pathlib import Path
import argparse
import typing

from hat import json

from hat.event.server.backends.lmdb import encoder
from hat.event.server.backends.lmdb.manager import common


def create_argument_parser(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser('query')
    parser.add_argument('--db', metavar='PATH', type=Path, required=True)
    subsubparsers = parser.add_subparsers(dest='subaction', required=True)

    last_event_id_parser = subsubparsers.add_parser('last_event_id')
    last_event_id_parser.add_argument('--server-id', type=int, default=None)

    partition_parser = subsubparsers.add_parser('partition')
    partition_parser.add_argument('--partition-id', type=int, default=None)

    latest_parser = subsubparsers.add_parser('latest')
    latest_parser.add_argument('--server-id', type=int, default=None)
    latest_parser.add_argument('--session-id', type=int, default=None)
    latest_parser.add_argument('--instance-id', type=int, default=None)
    latest_parser.add_argument('--event-type', type=str, default=None)

    ordered_parser = subsubparsers.add_parser('ordered')
    ordered_parser.add_argument('--partition-id', type=int, default=None)
    ordered_parser.add_argument('--server-id', type=int, default=None)
    ordered_parser.add_argument('--session-id', type=int, default=None)
    ordered_parser.add_argument('--instance-id', type=int, default=None)
    ordered_parser.add_argument('--event-type', type=str, default=None)

    return parser


def query(args) -> typing.Iterable[json.Data]:
    with common.ext_create_env(args.db, common.max_db_size) as env:
        if args.subaction == 'last_event_id':
            yield from _act_last_event_id(env, args.server_id)

        elif args.subaction == 'partition':
            yield from _act_partition(env, args.partition_id)

        elif args.subaction == 'latest':
            yield from _act_latest(env, args.server_id, args.session_id,
                                   args.instance_id, args.event_type)

        elif args.subaction == 'ordered':
            yield from _act_ordered(env, args.partition_id, args.server_id,
                                    args.session_id, args.instance_id,
                                    args.event_type)

        else:
            raise ValueError('unsupported subaction')


def _act_last_event_id(env, server_id):
    db = common.ext_open_db(env, common.DbType.SYSTEM)
    with env.begin(db=db, buffers=True) as txn:
        for encoded_key, encoded_value in txn.cursor():
            key = encoder.decode_system_db_key(encoded_key)
            value = encoder.decode_system_db_value(encoded_value)

            if server_id is not None and server_id != key:
                continue

            event_id, timestamp = value
            yield {'event_id': common.event_id_to_json(event_id),
                   'timestamp': common.timestamp_to_json(timestamp)}


def _act_partition(env, partition_id):
    db = common.ext_open_db(env, common.DbType.ORDERED_PARTITION)
    with env.begin(db=db, buffers=True) as txn:
        for encoded_key, encoded_value in txn.cursor():
            key = encoder.decode_ordered_partition_db_key(encoded_key)
            value = encoder.decode_ordered_partition_db_value(encoded_value)

            if partition_id is not None and partition_id != key:
                continue

            yield {'partition_id': key,
                   **value}


def _act_latest(env, server_id, session_id, instance_id, event_type):
    event_type = (tuple(event_type.split('/')) if event_type is not None
                  else None)

    db = common.ext_open_db(env, common.DbType.LATEST_DATA)
    with env.begin(db=db, buffers=True) as txn:
        for _, encoded_value in txn.cursor():
            value = encoder.decode_latest_data_db_value(encoded_value)

            if server_id is not None and server_id != value.event_id.server:
                continue

            if session_id is not None and session_id != value.event_id.session:
                continue

            if (instance_id is not None and
                    instance_id != value.event_id.instance):
                continue

            if (event_type is not None and
                    not common.matches_query_type(value.event_type,
                                                  event_type)):
                continue

            yield common.event_to_json(value)


def _act_ordered(env, partition_id, server_id, session_id, instance_id,
                 event_type):
    event_type = (tuple(event_type.split('/')) if event_type is not None
                  else None)

    db = common.ext_open_db(env, common.DbType.ORDERED_DATA)
    with env.begin(db=db, buffers=True) as txn:
        for encoded_key, encoded_value in txn.cursor():
            key = encoder.decode_ordered_data_db_key(encoded_key)
            value = encoder.decode_ordered_data_db_value(encoded_value)

            if partition_id is not None and partition_id != key[0]:
                continue

            if server_id is not None and server_id != value.event_id.server:
                continue

            if session_id is not None and session_id != value.event_id.session:
                continue

            if (instance_id is not None and
                    instance_id != value.event_id.instance):
                continue

            if (event_type is not None and
                    not common.matches_query_type(value.event_type,
                                                  event_type)):
                continue

            yield common.event_to_json(value)
