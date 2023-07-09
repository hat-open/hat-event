import functools
import typing

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder
from hat.event.server.backends.lmdb import environment


Changes: typing.TypeAlias = dict[common.SystemDbKey, common.SystemDbValue]


def ext_create(env: environment.Environment) -> 'SystemDb':
    db = SystemDb()
    db._env = env
    db._cache = {}
    db._changes = {}

    with env.ext_begin() as txn:
        with env.ext_cursor(txn, common.DbType.SYSTEM) as cursor:
            for encoded_key, encoded_value in cursor:
                key = encoder.decode_system_db_key(encoded_key)
                value = encoder.decode_system_db_value(encoded_value)
                db._cache[key] = value

    return db


class SystemDb(common.Flushable):

    def get_last_event_id_timestamp(self,
                                    server_id: common.ServerId
                                    ) -> tuple[common.EventId,
                                               common.Timestamp]:
        value = self._cache.get(server_id)
        if value:
            return value

        event_id = common.EventId(server=server_id,
                                  session=0,
                                  instance=0)
        timestamp = common.Timestamp(-(1 << 63), 0)
        return event_id, timestamp

    def set_last_event_id_timestamp(self,
                                    event_id: common.EventId,
                                    timestamp: common.Timestamp):
        self._cache[event_id.server] = event_id, timestamp
        self._changes[event_id.server] = event_id, timestamp

    def create_ext_flush(self) -> common.ExtFlushCb:
        changes, self._changes = self._changes, {}
        return functools.partial(self._ext_flush, changes)

    def _ext_flush(self, changes, txn):
        with self._env.ext_cursor(txn, common.DbType.SYSTEM) as cursor:
            for key, value in changes.items():
                encoded_key = encoder.encode_system_db_key(key)
                encoded_value = encoder.encode_system_db_value(value)
                cursor.put(encoded_key, encoded_value)

        return []
