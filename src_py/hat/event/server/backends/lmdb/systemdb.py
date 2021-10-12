import functools
import typing

import lmdb

from hat import aio
from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder


db_count = 1
db_name = b'system'


async def create(executor: aio.Executor,
                 env: lmdb.Environment,
                 server_id: int
                 ) -> 'SystemDb':
    return await executor(_ext_create, env, server_id)


def _ext_create(env, server_id):
    db = SystemDb()
    db._env = env
    db._server_id = server_id

    db._db = env.open_db(db_name)

    with env.begin(db=db._db, buffers=True) as txn:
        encoded_server_id = txn.get(b'server_id')
        encoded_last_instance_id = txn.get(b'last_instance_id')
        encoded_last_timestamp = txn.get(b'last_timestamp')

    if encoded_server_id:
        if encoder.decode_uint(encoded_server_id) != server_id:
            raise Exception('server id not matching')

    else:
        with env.begin(db=db._db, write=True) as txn:
            txn.put(b'server_id', encoder.encode_uint(server_id))

    db._last_instance_id = (encoder.decode_uint(encoded_last_instance_id)
                            if encoded_last_instance_id else None)

    db._last_timestamp = (encoder.decode_timestamp(encoded_last_timestamp)
                          if encoded_last_timestamp else None)

    return db


class SystemDb:

    @property
    def server_id(self) -> int:
        return self._server_id

    @property
    def last_instance_id(self) -> typing.Optional[int]:
        return self._last_instance_id

    @property
    def last_timestamp(self) -> typing.Optional[common.Timestamp]:
        return self._last_timestamp

    def change(self,
               last_instance_id: int,
               last_timestamp: common.Timestamp):
        self._last_instance_id = last_instance_id
        self._last_timestamp = last_timestamp

    def create_ext_flush(self) -> common.ExtFlushCb:
        return functools.partial(self._ext_flush, self._last_instance_id,
                                 self._last_timestamp)

    def _ext_flush(self, last_instance_id, last_timestamp, parent, now):
        with self._env.begin(db=self._db, parent=parent, write=True) as txn:
            if last_instance_id is not None:
                txn.put(b'last_instance_id',
                        encoder.encode_uint(last_instance_id))

            if last_timestamp is not None:
                txn.put(b'last_timestamp',
                        encoder.encode_timestamp(last_timestamp))
