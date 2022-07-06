import typing

import lmdb

from hat import aio
from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder


async def create(executor: aio.Executor,
                 env: lmdb.Environment
                 ) -> 'RefDb':
    return await executor(_ext_create, executor, env)


def _ext_create(executor, env):
    db = RefDb()
    db._executor = executor
    db._env = env

    db._ref_db = common.ext_open_db(env, common.DbType.REF)
    db._latest_db = common.ext_open_db(env, common.DbType.LATEST_DATA)
    db._ordered_db = common.ext_open_db(env, common.DbType.ORDERED_DATA)

    return db


class RefDb(common.Flushable):

    async def query(self,
                    event_id: common.EventId
                    ) -> typing.AsyncIterable[typing.List[common.Event]]:
        pass

    def create_ext_flush(self) -> common.ExtFlushCb:
        return self._ext_flush

    def _ext_flush(self, ctx):
        with self._env.begin(db=self._ref_db,
                             parent=ctx.transaction,
                             write=True) as txn:
            for change in ctx.get_changes():
                key = change.event_id
                encoded_key = encoder.encode_ref_db_key(key)

                encoded_value = txn.pop(encoded_key)
                value = (encoder.decode_ref_db_value(encoded_value)
                         if encoded_value else set())

                value = value + change.added - change.removed
                if not value:
                    continue
                encoded_value = encoder.encode_ref_db_value(value)

                txn.put(encoded_key, encoded_value)
