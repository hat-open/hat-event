import typing

import lmdb

from hat.event.backends.lmdb import common


version = '0.9'

Timestamp = common.Timestamp
OrderBy = common.OrderBy
EventId = common.EventId
EventType = common.EventType
EventPayloadBinary = common.EventPayloadBinary
EventPayloadJson = common.EventPayloadJson
Event = common.Event

DbKey = common.DbKey
DbValue = common.DbValue
DbType = common.DbType
SettingsId = common.SettingsId
LatestEventRef = common.LatestEventRef
TimeseriesEventRef = common.TimeseriesEventRef
db_defs = common.db_defs
create_env = common.ext_create_env
open_db = common.ext_open_db


def read(dbs: dict[DbType, lmdb._Database],
         txn: lmdb.Transaction,
         db_type: DbType
         ) -> typing.Iterable[tuple[DbKey, DbValue]]:
    db_def = db_defs[db_type]
    with txn.cursor(dbs[db_type]) as cursor:
        for encoded_key, encoded_value in cursor:
            key = db_def.decode_key(encoded_key)
            value = db_def.decode_value(encoded_value)
            yield key, value


def write(env: lmdb.Environment,
          dbs: dict[DbType, lmdb._Database],
          db_type: DbType,
          key: DbKey,
          value: DbValue):
    db_def = db_defs[db_type]
    encoded_key = db_def.encode_key(key)
    encoded_value = db_def.encode_value(value)

    with env.begin(write=True) as txn:
        txn.put(encoded_key, encoded_value, db=dbs[db_type])
