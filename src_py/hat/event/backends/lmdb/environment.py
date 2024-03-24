from pathlib import Path
import asyncio
import typing

import lmdb

from hat import aio

from hat.event.backends.lmdb import common


async def create(db_path: Path,
                 max_size: int = common.default_max_size
                 ) -> 'Environment':
    env = Environment()
    env._loop = asyncio.get_running_loop()
    env._async_group = aio.Group()
    env._executor = aio.Executor(1, log_exceptions=False)
    env._env = None
    env._dbs = {}

    env.async_group.spawn(aio.call_on_cancel, env._on_close)

    try:
        await env._executor.spawn(env._ext_init, db_path, max_size)

    except BaseException:
        await aio.uncancellable(env.async_close())
        raise

    return env


class Environment(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def execute(self,
                      ext_fn: typing.Callable,
                      *args: typing.Any) -> typing.Any:
        if not self.is_open:
            raise Exception('environment closed')

        return await self._executor.spawn(ext_fn, *args)

    def ext_begin(self, write: bool = False) -> lmdb.Transaction:
        return self._env.begin(write=write,
                               buffers=True)

    def ext_cursor(self,
                   txn: lmdb.Transaction,
                   db_type: common.DbType
                   ) -> lmdb.Cursor:
        return txn.cursor(self._dbs[db_type])

    def ext_stat(self,
                 txn: lmdb.Transaction,
                 db_type: common.DbType
                 ) -> dict[str, int]:
        return txn.stat(self._dbs[db_type])

    def ext_read(self,
                 txn: lmdb.Transaction,
                 db_type: common.DbType
                 ) -> typing.Iterable[tuple[common.DbKey, common.DbValue]]:
        db_def = common.db_defs[db_type]
        with txn.cursor(self._dbs[db_type]) as cursor:
            for encoded_key, encoded_value in cursor:
                key = db_def.decode_key(encoded_key)
                value = db_def.decode_value(encoded_value)
                yield key, value

    def ext_write(self,
                  txn: lmdb.Transaction,
                  db_type: common.DbType,
                  data: typing.Iterable[tuple[common.DbKey, common.DbValue]]):
        db_def = common.db_defs[db_type]
        with txn.cursor(self._dbs[db_type]) as cursor:
            for key, value in data:
                encoded_key = db_def.encode_key(key)
                encoded_value = db_def.encode_value(value)
                cursor.put(encoded_key, encoded_value)

    async def _on_close(self):
        if self._env:
            await self._executor.spawn(self._env.close)

        await self._executor.async_close()

    def _ext_init(self, db_path, max_size):
        create = not db_path.exists()

        self._env = common.ext_create_env(path=db_path,
                                          max_size=max_size)
        self._dbs = {db_type: common.ext_open_db(env=self._env,
                                                 db_type=db_type,
                                                 create=create)
                     for db_type in common.DbType}
