from pathlib import Path
import argparse

from hat import json

from hat.event.backends.lmdb.manager import common


def create_argument_parser(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser('query')
    parser.add_argument('--db', metavar='PATH', type=Path, required=True)
    subsubparsers = parser.add_subparsers(dest='subaction', required=True)

    subsubparsers.add_parser('settings')

    last_event_id_parser = subsubparsers.add_parser('last_event_id')
    last_event_id_parser.add_argument('--server-id', type=int, default=None)

    last_timestamps_parser = subsubparsers.add_parser('last_timestamp')
    last_timestamps_parser.add_argument('--server-id', type=int, default=None)

    ref_parser = subsubparsers.add_parser('ref')
    ref_parser.add_argument('--server-id', type=int, default=None)

    latest_parser = subsubparsers.add_parser('latest')
    latest_parser.add_argument('--event-types', type=str, default=None,
                               nargs='*')

    partition_parser = subsubparsers.add_parser('partition')
    partition_parser.add_argument('--partition-id', type=int, default=None)

    timeseries_parser = subsubparsers.add_parser('timeseries')
    timeseries_parser.add_argument('--partition-id', type=int, default=None)
    timeseries_parser.add_argument('--server-id', type=int, default=None)
    timeseries_parser.add_argument('--event-types', type=str, default=None,
                                   nargs='*')

    return parser


def query(args):
    with common.ext_create_env(args.db, readonly=True) as env:
        dbs = {db_type: common.ext_open_db(env, db_type, False)
               for db_type in common.DbType}

        with env.begin(buffers=True) as txn:
            if args.subaction == 'settings':
                for result in _query_settings(dbs, txn):
                    _print_result(result)

            elif args.subaction == 'last_event_id':
                for result in _query_last_event_id(dbs, txn, args.server_id):
                    _print_result(result)

            elif args.subaction == 'last_timestamp':
                for result in _query_last_timestamp(dbs, txn, args.server_id):
                    _print_result(result)

            elif args.subaction == 'ref':
                for result in _query_ref(dbs, txn):
                    _print_result(result)

            elif args.subaction == 'latest':
                event_types = ([tuple(i.split('/')) for i in args.event_types]
                               if args.event_types is not None else None)
                for result in _query_ref(dbs, txn, event_types):
                    _print_result(result)

            elif args.subaction == 'partition':
                for result in _query_partition(dbs, txn, args.partition_id):
                    _print_result(result)

            elif args.subaction == 'timeseries':
                event_types = ([tuple(i.split('/')) for i in args.event_types]
                               if args.event_types is not None else None)
                for result in _query_timeseries(dbs, txn, args.partition_id,
                                                args.server_id, event_types):
                    _print_result(result)

            else:
                raise ValueError('unsupported subaction')


def _print_result(result):
    print(json.encode(result))


def _query_settings(dbs, txn):
    db_type = common.DbType.SYSTEM_SETTINGS
    db = dbs[db_type]
    db_def = common.db_defs[db_type]

    for key, value in txn.cursor(db=db):
        settings_id = db_def.decode_key(key)
        data = db_def.decode_value(value)

        yield {'settings_id': settings_id.name,
               'data': data}


def _query_last_event_id(dbs, txn, server_id):
    db_type = common.DbType.SYSTEM_LAST_EVENT_ID
    db = dbs[db_type]
    db_def = common.db_defs[db_type]

    for key, value in txn.cursor(db=db):
        decoded_server_id = db_def.decode_key(key)
        if server_id is not None and server_id != decoded_server_id:
            continue

        event_id = db_def.decode_value(value)
        yield common.event_id_to_json(event_id)


def _query_last_timestamp(dbs, txn, server_id):
    db_type = common.DbType.SYSTEM_LAST_TIMESTAMP
    db = dbs[db_type]
    db_def = common.db_defs[db_type]

    for key, value in txn.cursor(db=db):
        decoded_server_id = db_def.decode_key(key)
        if server_id is not None and server_id != decoded_server_id:
            continue

        timestamp = db_def.decode_value(value)
        yield common.timestamp_to_json(timestamp)


def _query_ref(dbs, txn, server_id):
    ref_db_type = common.DbType.REF
    ref_db = dbs[ref_db_type]
    ref_db_def = common.db_defs[ref_db_type]

    latest_type_db_type = common.DbType.LATEST_TYPE
    latest_type_db = dbs[latest_type_db_type]
    latest_type_db_def = common.db_defs[latest_type_db_type]

    for key, value in txn.cursor(db=ref_db):
        event_id = ref_db_def.decode_key(key)
        if server_id is not None and event_id.server != server_id:
            continue

        event_refs = ref_db_def.decode_value(value)

        for event_ref in event_refs:
            if isinstance(event_ref, common.LatestEventRef):
                latest_type_key = latest_type_db_def.encoded_key(event_ref.key)
                latest_type_value = txn.get(latest_type_key, db=latest_type_db)

                event_type = latest_type_db_def.decode_value(latest_type_value)
                yield {'event_id': common.event_id_to_json(event_id),
                       'ref_type': 'latest',
                       'event_type': list(event_type)}

            if isinstance(event_ref, common.TimeseriesEventRef):
                partition_id, timestamp, _ = event_ref.key
                yield {'event_id': common.event_id_to_json(event_id),
                       'ref_type': 'timeseries',
                       'partition_id': partition_id,
                       'timestamp': common.timestamp_to_json(timestamp)}


def _query_latest(dbs, txn, event_types):
    db_type = common.DbType.LATEST_DATA
    db = dbs[db_type]
    db_def = common.db_defs[db_type]

    subscription = common.create_subscription([('*', )] if event_types is None
                                              else event_types)

    for _, value in txn.cursor(db=db):
        event = db_def.decode_value(value)
        if not subscription.matches(event.type):
            continue

        yield common.event_to_json(value)


def _query_partition(dbs, txn, partition_id):
    db_type = common.DbType.TIMESERIES_PARTITION
    db = dbs[db_type]
    db_def = common.db_defs[db_type]

    for key, value in txn.cursor(db=db):
        decoded_partition_id = db_def.decode_key(key)
        if partition_id is not None and partition_id != decoded_partition_id:
            continue

        data = db_def.decode_value(value)
        yield {'partition_id': decoded_partition_id,
               'data': data}


def _query_timeseries(dbs, txn, partition_id, server_id, event_types):
    db_type = common.DbType.TIMESERIES_DATA
    db = dbs[db_type]
    db_def = common.db_defs[db_type]

    subscription = common.create_subscription([('*', )] if event_types is None
                                              else event_types)

    for key, value in txn.cursor(db=db):
        decoded_partition_id, _, event_id = db_def.decode(key)

        if partition_id is not None and partition_id != decoded_partition_id:
            continue

        if server_id is not None and server_id != event_id.server:
            continue

        event = db_def.decode_value(value)
        if not subscription.matches(event.type):
            continue

        yield common.event_to_json(value)
