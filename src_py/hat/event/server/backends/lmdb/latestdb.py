import functools
import typing

import lmdb

from hat import aio
from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder
from hat.event.server.backends.lmdb.conditions import Conditions


db_count = 1
db_name = b'latest'


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
    db._changes = {}

    db._db = env.open_db(db_name)

    db._events = {}
    with env.begin(db=db._db, buffers=True) as txn:
        for _, value in txn.cursor():
            event = encoder.decode_event(value)
            if not subscription.matches(event.event_type):
                continue
            if not conditions.matches(event):
                continue
            db._events[event.event_type] = event

    return db


class LatestDb:

    @property
    def subscription(self) -> common.Subscription:
        return self._subscription

    def add(self, event: common.Event) -> bool:
        if not self._subscription.matches(event.event_type):
            return False

        key, value = event.event_type, event
        self._events[key] = value
        self._changes[key] = value
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
        changes, self._changes = self._changes, {}
        return functools.partial(self._ext_flush, changes)

    def _ext_flush(self, changes, parent, now):
        with self._env.begin(db=self._db, parent=parent, write=True) as txn:
            for key, value in changes.items():
                txn.put(encoder.encode_tuple_str(key),
                        encoder.encode_event(value))
