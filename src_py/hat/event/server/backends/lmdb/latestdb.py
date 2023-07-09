import functools
import typing

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder
from hat.event.server.backends.lmdb import environment
from hat.event.server.backends.lmdb import refdb
from hat.event.server.backends.lmdb.conditions import Conditions


Changes: typing.TypeAlias = tuple[dict[common.LatestDataDbKey,
                                       common.LatestDataDbValue],
                                  dict[common.LatestTypeDbKey,
                                       common.LatestTypeDbValue]]


def ext_create(env: environment.Environment,
               ref_db: refdb.RefDb,
               subscription: common.Subscription,
               conditions: Conditions
               ) -> 'LatestDb':
    db = LatestDb()
    db._env = env
    db._ref_db = ref_db
    db._subscription = subscription
    db._conditions = conditions
    db._changes = {}, {}
    db._events = {}
    db._type_refs = {}

    with env.ext_begin() as txn:
        with env.ext_cursor(txn, common.DbType.LATEST_DATA) as cursor:
            for _, encoded_value in cursor:
                event = encoder.decode_latest_data_db_value(encoded_value)
                if not subscription.matches(event.event_type):
                    continue
                if not conditions.matches(event):
                    continue
                db._events[event.event_type] = event

        with env.ext_cursor(txn, common.DbType.LATEST_TYPE) as cursor:
            for encoded_key, encoded_value in cursor:
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
              event_types: list[common.EventType] | None
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

    def _ext_flush(self, changes, txn):
        with self._env.ext_cursor(txn, common.DbType.LATEST_DATA) as cursor:
            for key, value in changes[0].items():
                event_ref = common.LatestEventRef(key)
                encoded_key = encoder.encode_latest_data_db_key(key)
                encoded_value = encoder.encode_latest_data_db_value(value)

                previous_encoded_value = cursor.get(encoded_key)
                if previous_encoded_value:
                    previous_value = encoder.decode_latest_data_db_value(
                        previous_encoded_value)
                    self._ref_db.ext_remove_event_ref(
                        txn, previous_value.event_id, event_ref)

                cursor.put(encoded_key, encoded_value)
                self._ref_db.ext_add_event_ref(txn, value.event_id, event_ref)

        with self._env.ext_cursor(txn, common.DbType.LATEST_TYPE) as cursor:
            for key, value in changes[1].items():
                encoded_key = encoder.encode_latest_type_db_key(key)
                encoded_value = encoder.encode_latest_type_db_value(value)

                cursor.put(encoded_key, encoded_value)

        return changes[0].values()
