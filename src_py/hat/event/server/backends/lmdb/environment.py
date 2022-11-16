from pathlib import Path
import typing

import lmdb

from hat import aio
from hat.event.server.backends.lmdb import common


async def create(db_path: Path,
                 max_db_size: int
                 ) -> 'Environment':
    env = Environment()
    env._async_group = aio.Group()
    env._executor = aio.create_executor(1)

    env._env = await env._executor(common.ext_create_env, db_path, max_db_size)
    env.async_group.spawn(aio.call_on_cancel, env._executor, env._env.close)

    env._dbs = {}
    try:
        for db_type in common.DbType:
            env._dbs[db_type] = await env._executor(common.ext_open_db,
                                                    env._env, db_type)

    except BaseException:
        await aio.uncancellable(env.async_close())
        raise

    return env


class Environment(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def execute(self, *args: typing.Any) -> typing.Any:
        return await self.async_group.spawn(self._executor, *args)

    def ext_begin(self,
                  db_type: typing.Optional[common.DbType] = None,
                  parent: typing.Optional[lmdb.Transaction] = None,
                  write: bool = False,
                  buffers: bool = True,
                  ) -> lmdb.Transaction:
        db = self._dbs[db_type] if db_type is not None else None
        return self._env.begin(db=db,
                               parent=parent,
                               write=write,
                               buffers=buffers)

    def ext_stat(self,
                 txn: lmdb.Transaction,
                 db_type: common.DbType
                 ) -> typing.Dict[str, int]:
        return txn.stat(self._dbs[db_type])
