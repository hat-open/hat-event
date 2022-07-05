import typing
import enum
import struct
import itertools

import lmdb

from hat import aio
from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import latestdb
from hat.event.server.backends.lmdb import ordereddb


db_count = 1
db_name = b'ref'

Source = enum.Enum('Source', [('LATEST', 0),
                              ('ORDERED', 1)])
Ref = typing.Tuple[Source, typing.Union[latestdb.DataKey,
                                        ordereddb.DataKey]]

Key = common.EventId
Value = typing.Set[Ref]


def encode_key(key: Key) -> bytes:
    return struct.pack(">QQQ", key.server, key.session, key.instance)


def decode_key(key_bytes: bytes) -> Key:
    server_id, session_id, instance_id, *_ = struct.unpack(">QQQ", key_bytes)
    return common.EventId(server=server_id,
                          session=session_id,
                          instance=instance_id)


def encode_value(value: Value) -> bytes:
    return bytes(itertools.chain.from_iterable(_encode_ref(i) for i in value))


def decode_value(value_bytes: bytes) -> Value:
    value = set()
    while value_bytes:
        ref, value_bytes = _decode_ref(value_bytes)
        value.add(ref)
    return value


def _encode_ref(ref):
    source, key = ref
    yield source.value

    if source == Source.LATEST:
        yield from latestdb.encode_key(key)

    elif source == Source.ORDERED:
        yield from ordereddb.encode_key(key)

    else:
        raise ValueError('unsupported source')


def _decode_ref(ref_bytes):
    source = Source(ref_bytes[0])

    if source == Source.LATEST:
        ref = source, latestdb.decode_data_key(ref_bytes[1:9])
        return ref, ref_bytes[9:]

    if source == Source.ORDERED:
        ref = source, ordereddb.decode_data_key(ref_bytes[1:45])
        return ref, ref_bytes[45:]

    raise ValueError('unsupported source')


async def create(executor: aio.Executor,
                 env: lmdb.Environment
                 ) -> 'RefDb':
    return await executor(_ext_create, executor, env)


def _ext_create(executor, env):
    db = RefDb()
    db._executor = executor
    db._env = env

    db._ref_db = env.open_db(db_name)
    db._latest_db = env.open_db(latestdb.data_db_name)
    db._ordered_db = env.open_db(ordereddb.data_db_name)

    return db


class RefDb:

    async def query_from_event_id(self,
                                  event_id: common.EventId
                                  ) -> typing.AsyncIterable[typing.List[common.Event]]:  # NOQA
        pass

    def ext_add_latest_key(self,
                           parent: lmdb.Transaction,
                           event_id: common.EventId,
                           key: latestdb.DataKey):
        pass

    def ext_remove_latest_key(self,
                              parent: lmdb.Transaction,
                              event_id: common.EventId,
                              key: latestdb.DataKey):
        pass

    def ext_add_ordered_key(self,
                            parent: lmdb.Transaction,
                            event_id: common.EventId,
                            key: ordereddb.DataKey):
        pass

    def ext_remove_ordered_key(self,
                               parent: lmdb.Transaction,
                               event_id: common.EventId,
                               key: ordereddb.DataKey):
        pass
