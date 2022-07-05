import functools
import struct
import typing

import lmdb

from hat import aio
from hat import json
from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb.conditions import Conditions


db_count = 2
data_db_name = b'latest_data'
type_db_name = b'latest_type'

EventTypeRef = int

DataKey = EventTypeRef
DataValue = common.Event

TypeKey = EventTypeRef
TypeValue = common.EventType

Changes = typing.Tuple[typing.Dict[DataKey, DataValue],
                       typing.Dict[TypeKey, TypeValue]]


def encode_data_key(key: DataKey) -> bytes:
    return struct.pack(">Q", key)


def decode_data_key(key_bytes: bytes) -> DataKey:
    return struct.unpack(">Q", key_bytes)[0]


def encode_data_value(value: DataValue) -> bytes:
    event_sbs = common.event_to_sbs(value)
    return common.sbs_repo.encode('HatEvent', 'Event', event_sbs)


def decode_data_value(value_bytes: bytes) -> DataValue:
    event_sbs = common.sbs_repo.decode('HatEvent', 'Event', value_bytes)
    return common.event_from_sbs(event_sbs)


def encode_type_key(key: TypeKey) -> bytes:
    return struct.pack(">Q", key)


def decode_type_key(key_bytes: bytes) -> TypeKey:
    return struct.unpack(">Q", key_bytes)[0]


def encode_type_value(value: TypeValue) -> bytes:
    return json.encode(list(value)).encode('utf-8')


def decode_type_value(value_bytes: bytes) -> TypeValue:
    return tuple(json.decode(str(value_bytes, encoding='utf-8')))


async def create(executor: aio.Executor,
                 env: lmdb.Environment,
                 subscription: common.Subscription,
                 conditions: Conditions
                 ) -> 'LatestDb':
    return await executor(_ext_create, env, subscription, conditions)


def _ext_create(env, subscription, conditions):
    db = LatestDb()
    db._env = env
    db._subscription = subscription
    db._conditions = conditions
    db._changes = ({}, {})

    db._data_db = env.open_db(data_db_name)
    db._type_db = env.open_db(type_db_name)

    db._events = {}
    with env.begin(db=db._data_db, buffers=True) as txn:
        for _, value in txn.cursor():
            event = decode_data_value(value)
            if not subscription.matches(event.event_type):
                continue
            if not conditions.matches(event):
                continue
            db._events[event.event_type] = event

    db._type_refs = {}
    with env.begin(db=db._type_db, buffers=True) as txn:
        for key, value in txn.cursor():
            ref = decode_type_key(key)
            event_type = decode_type_value(value)
            db._type_refs[event_type] = ref

    return db


class LatestDb:

    @property
    def subscription(self) -> common.Subscription:
        return self._subscription

    def add(self, event: common.Event) -> bool:
        if not self._subscription.matches(event.event_type):
            return False

        ref = self._type_refs.get(event.event_type)
        if ref is None:
            ref = len(self._type_refs) + 1
            self._type_refs[event.event_type] = ref
            self._changes[1][ref] = event.event_type

        self._events[event.event_type] = event
        self._changes[0][ref] = event

        return True

    def query(self,
              event_types: typing.Optional[typing.List[common.EventType]]
              ) -> typing.Iterable[common.Event]:
        if event_types is None:
            yield from self._events.values()

        elif any(any(subtype in ('*', '?')
                     for subtype in event_type)
                 for event_type in event_types):
            subscription = common.Subscription(event_types)
            for event_type, event in self._events.items():
                if subscription.matches(event_type):
                    yield event

        else:
            for event_type in event_types:
                event = self._events.get(event_type)
                if event:
                    yield event

    def create_ext_flush(self) -> common.ExtFlushCb:
        changes, self._changes = self._changes, ({}, {})
        return functools.partial(self._ext_flush, changes)

    def _ext_flush(self, changes, parent, now):
        with self._env.begin(db=self._data_db,
                             parent=parent,
                             write=True) as txn:
            for key, value in changes[0].items():
                txn.put(encode_data_key(key),
                        encode_data_value(value))

        with self._env.begin(db=self._type_db,
                             parent=parent,
                             write=True) as txn:
            for key, value in changes[1].items():
                txn.put(encode_type_key(key),
                        encode_type_value(value))
