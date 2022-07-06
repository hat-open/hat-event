import functools
import typing

import lmdb

from hat import aio
from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder


Changes = typing.Dict[common.SystemDbKey, common.SystemDbValue]


async def create(executor: aio.Executor,
                 env: lmdb.Environment,
                 ) -> 'SystemDb':
    return await executor(_ext_create, env)


def _ext_create(env):
    db = SystemDb()
    db._env = env
    db._cache = {}
    db._changes = {}

    db._db = common.ext_open_db(env, common.DbType.SYSTEM)

    with env.begin(db=db._db, buffers=True) as txn:
        for encoded_key, encoded_value in txn.cursor():
            key = encoder.decode_system_db_key(encoded_key)
            value = encoder.decode_system_db_value(encoded_value)
            db._cache[key] = value

    return db


class SystemDb(common.Flushable):

    def get_last_event_id_timestamp(self,
                                    server_id: common.ServerId
                                    ) -> typing.Tuple[common.EventId,
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

    def _ext_flush(self, changes, ctx):
        with self._env.begin(db=self._db,
                             parent=ctx.transaction,
                             write=True) as txn:
            for key, value in changes.items():
                encoded_key = encoder.encode_system_db_key(key)
                encoded_value = encoder.encode_system_db_value(value)
                txn.put(encoded_key, encoded_value)
