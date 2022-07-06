import functools
import typing

import lmdb

from hat import aio
from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder
from hat.event.server.backends.lmdb.conditions import Conditions


Changes = typing.Tuple[typing.Dict[common.LatestDataDbKey,
                                   common.LatestDataDbValue],
                       typing.Dict[common.LatestTypeDbKey,
                                   common.LatestTypeDbValue]]


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
    db._changes = {}, {}

    db._data_db = common.ext_open_db(env, common.DbType.LATEST_DATA)
    db._type_db = common.ext_open_db(env, common.DbType.LATEST_TYPE)

    db._events = {}
    with env.begin(db=db._data_db, buffers=True) as txn:
        for _, encoded_value in txn.cursor():
            event = encoder.decode_latest_data_db_value(encoded_value)
            if not subscription.matches(event.event_type):
                continue
            if not conditions.matches(event):
                continue
            db._events[event.event_type] = event

    db._type_refs = {}
    with env.begin(db=db._type_db, buffers=True) as txn:
        for encoded_key, encoded_value in txn.cursor():
            ref = encoder.decode_latest_type_db_key(encoded_key)
            event_type = encoder.decode_latest_type_db_value(encoded_value)
            db._type_refs[event_type] = ref

    return db


class LatestDb(common.Flushable):

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

    def _ext_flush(self, changes, ctx):
        with self._env.begin(db=self._data_db,
                             parent=ctx.transaction,
                             write=True) as txn:
            for key, value in changes[0].items():
                event_ref = common.LatestEventRef(key)
                encoded_key = encoder.encode_latest_data_db_key(key)
                encoded_value = encoder.encode_latest_data_db_value(value)

                previous_encoded_value = txn.get(encoded_key)
                if previous_encoded_value:
                    previous_value = encoder.decode_latest_data_db_value(
                        previous_encoded_value)
                    ctx.remove_event_ref(previous_value.event_id, event_ref)

                txn.put(encoded_key, encoded_value)
                ctx.add_event_ref(value, event_ref)

        with self._env.begin(db=self._type_db,
                             parent=ctx.transaction,
                             write=True) as txn:
            for key, value in changes[1].items():
                encoded_key = encoder.encode_latest_type_db_key(key)
                encoded_value = encoder.encode_latest_type_db_value(value)

                txn.put(encoded_key, encoded_value)
