from pathlib import Path
import argparse

from hat.event.backends.lmdb.manager import common


def create_argument_parser(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser('copy')
    parser.add_argument('--skip-latest', action='store_true')
    parser.add_argument('--skip-timeseries', action='store_true')
    parser.add_argument('src_path', type=Path)
    parser.add_argument('dst_path', type=Path)
    return parser


def copy(args):
    if args.dst_path.exist():
        raise Exception('destination db already exists')

    with common.ext_create_env(args.src_path, readonly=True) as src_env:
        with common.ext_create_env(args.dst_path) as dst_env:
            src_dbs = {db_type: common.ext_open_db(src_env, db_type, False)
                       for db_type in common.DbType}
            dst_dbs = {db_type: common.ext_open_db(dst_env, db_type, True)
                       for db_type in common.DbType}

            with src_env.begin(buffers=True) as src_txn:
                _copy_system(src_dbs, src_txn, dst_env, dst_dbs)
                _copy_ref(src_dbs, src_txn, dst_env, dst_dbs,
                          args.skip_latest, args.skip_timeseries)

                if not args.skip_latest:
                    _copy_latest(src_dbs, src_txn, dst_env, dst_dbs)

                if not args.skip_timeseries:
                    _copy_timeseries(src_dbs, src_txn, dst_env, dst_dbs)


def _copy_system(src_dbs, src_txn, dst_env, dst_dbs):
    for db_type in [common.DbType.SYSTEM_SETTINGS,
                    common.DbType.SYSTEM_LAST_EVENT_ID,
                    common.DbType.SYSTEM_LAST_TIMESTAMP]:
        _copy_db(src_dbs[db_type], src_txn, dst_env, dst_dbs[db_type])


def _copy_ref(src_dbs, src_txn, dst_env, dst_dbs, skip_latest,
              skip_timeseries):
    db_type = common.DbType.REF
    db_def = common.db_defs[db_type]

    for key, value in src_txn.cursor(db=src_dbs[db_type]):
        with dst_env.begin(db=dst_dbs[db_type], write=True) as dst_txn:
            if not skip_latest and not skip_timeseries:
                dst_txn.put(key, value)
                continue

            event_refs = db_def.decode_value(value)

            if skip_latest:
                event_refs = (
                    event_ref for event_ref in event_refs
                    if not isinstance(event_ref, common.LatestEventRef))

            if skip_timeseries:
                event_refs = (
                    event_ref for event_ref in event_refs
                    if not isinstance(event_ref, common.TimeseriesEventRef))

            event_refs = set(event_refs)
            if not event_refs:
                continue

            value = db_def.encode_value(event_refs)
            dst_txn.put(key, value)


def _copy_latest(src_dbs, src_txn, dst_env, dst_dbs):
    for db_type in [common.DbType.LATEST_DATA,
                    common.DbType.LATEST_TYPE]:
        _copy_db(src_dbs[db_type], src_txn, dst_env, dst_dbs[db_type])


def _copy_timeseries(src_dbs, src_txn, dst_env, dst_dbs):
    for db_type in [common.DbType.TIMESERIES_DATA,
                    common.DbType.TIMESERIES_PARTITION,
                    common.DbType.TIMESERIES_COUNT]:
        _copy_db(src_dbs[db_type], src_txn, dst_env, dst_dbs[db_type])


def _copy_db(src_db, src_txn, dst_env, dst_db):
    for key, value in src_txn.cursor(db=src_db):
        with dst_env.begin(db=dst_db, write=True) as dst_txn:
            dst_txn.put(key, value)
