import collections
import functools
import itertools
import typing

import lmdb

from hat import aio
from hat import json
from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder
from hat.event.server.backends.lmdb.conditions import Conditions


db_count = 3
data_db_name = b'ordered_data'
partition_db_name = b'ordered_partition'
count_db_name = b'ordered_count'


async def create(executor: aio.Executor,
                 env: lmdb.Environment,
                 subscription: common.Subscription,
                 conditions: Conditions,
                 order_by: common.OrderBy,
                 limit: typing.Optional[json.Data]
                 ) -> 'OrderedDb':
    return await executor(_ext_create, executor, env, subscription, conditions,
                          order_by, limit)


def _ext_create(executor, env, subscription, conditions, order_by, limit):
    db = OrderedDb()
    db._executor = executor
    db._env = env
    db._subscription = subscription
    db._conditions = conditions
    db._order_by = order_by
    db._limit = limit
    db._changes = collections.deque()

    db._data_db = env.open_db(data_db_name)
    db._count_db = env.open_db(count_db_name)

    db._partition_id = None
    last_partition_id = 0
    partition_data = {
        'order': order_by.value,
        'subscriptions': [list(i)
                          for i in sorted(subscription.get_query_types())]}

    partition_db = env.open_db(partition_db_name)

    with env.begin(db=partition_db, buffers=True) as txn:
        for key, value in txn.cursor():
            last_partition_id = encoder.decode_uint(key)
            if encoder.decode_json(value) == partition_data:
                db._partition_id = last_partition_id
                break

    if db._partition_id is None:
        db._partition_id = last_partition_id + 1
        with env.begin(db=partition_db, write=True) as txn:
            txn.put(encoder.encode_uint(db._partition_id),
                    encoder.encode_json(partition_data))

    return db


class OrderedDb:

    @property
    def partition_id(self) -> int:
        return self._partition_id

    @property
    def subscription(self) -> common.Subscription:
        return self._subscription

    @property
    def order_by(self) -> common.OrderBy:
        return self._order_by

    def add(self, event: common.Event) -> bool:
        if not self._subscription.matches(event.event_type):
            return False

        if self._order_by == common.OrderBy.TIMESTAMP:
            timestamp = event.timestamp

        elif self._order_by == common.OrderBy.SOURCE_TIMESTAMP:
            if event.source_timestamp is None:
                return False
            timestamp = event.source_timestamp

        else:
            raise ValueError('unsupported order by')

        self._changes.append((timestamp, event))
        return True

    async def query(self,
                    subscription: typing.Optional[common.Subscription],
                    event_ids: typing.Optional[typing.List[common.EventId]],
                    t_from: typing.Optional[common.Timestamp],
                    t_to: typing.Optional[common.Timestamp],
                    source_t_from: typing.Optional[common.Timestamp],
                    source_t_to: typing.Optional[common.Timestamp],
                    payload: typing.Optional[common.EventPayload],
                    order: common.Order,
                    unique_type: bool,
                    max_results: typing.Optional[int]
                    ) -> typing.Iterable[common.Event]:
        unique_types = set() if unique_type else None
        events = collections.deque()

        if order == common.Order.DESCENDING:
            events.extend(self._query_changes(
                subscription, event_ids, t_from, t_to, source_t_from,
                source_t_to, payload, order, unique_types, max_results))

            if max_results is not None:
                max_results -= len(events)
                if max_results <= 0:
                    return events

            events.extend(await self._executor(
                self._ext_query, subscription, event_ids, t_from, t_to,
                source_t_from, source_t_to, payload, order, unique_types,
                max_results))

        elif order == common.Order.ASCENDING:
            events.extend(await self._executor(
                self._ext_query, subscription, event_ids, t_from, t_to,
                source_t_from, source_t_to, payload, order, unique_types,
                max_results))

            if max_results is not None:
                max_results -= len(events)
                if max_results <= 0:
                    return events

            events.extend(self._query_changes(
                subscription, event_ids, t_from, t_to, source_t_from,
                source_t_to, payload, order, unique_types, max_results))

        else:
            raise ValueError('unsupported order')

        return events

    def create_ext_flush(self) -> common.ExtFlushCb:
        changes, self._changes = self._changes, collections.deque()
        return functools.partial(self._ext_flush, changes)

    def _query_changes(self, subscription, event_ids, t_from, t_to,
                       source_t_from, source_t_to, payload, order,
                       unique_types, max_results):
        if order == common.Order.DESCENDING:
            events = (event for _, event in reversed(self._changes))

            if (self._order_by == common.OrderBy.TIMESTAMP and
                    t_to is not None):
                events = itertools.dropwhile(
                    lambda i: t_to < i.timestamp,
                    events)

            elif (self._order_by == common.OrderBy.SOURCE_TIMESTAMP and
                    source_t_to is not None):
                events = itertools.dropwhile(
                    lambda i: source_t_to < i.source_timestamp,
                    events)

            if (self._order_by == common.OrderBy.TIMESTAMP and
                    t_from is not None):
                events = itertools.takewhile(
                    lambda i: t_from <= i.timestamp,
                    events)

            elif (self._order_by == common.OrderBy.SOURCE_TIMESTAMP and
                    source_t_from is not None):
                events = itertools.takewhile(
                    lambda i: source_t_from <= i.source_timestamp,
                    events)

        elif order == common.Order.ASCENDING:
            events = (event for _, event in self._changes)

            if (self._order_by == common.OrderBy.TIMESTAMP and
                    t_from is not None):
                events = itertools.dropwhile(
                    lambda i: i.timestamp < t_from,
                    events)

            elif (self._order_by == common.OrderBy.SOURCE_TIMESTAMP and
                    source_t_from is not None):
                events = itertools.dropwhile(
                    lambda i: i.source_timestamp < source_t_from,
                    events)

            if (self._order_by == common.OrderBy.TIMESTAMP and
                    t_to is not None):
                events = itertools.takewhile(
                    lambda i: i.timestamp <= t_to,
                    events)

            elif (self._order_by == common.OrderBy.SOURCE_TIMESTAMP and
                    source_t_to is not None):
                events = itertools.takewhile(
                    lambda i: i.source_timestamp <= source_t_to,
                    events)

        else:
            raise ValueError('unsupported order')

        yield from _filter_events(events, subscription, event_ids, t_from,
                                  t_to, source_t_from, source_t_to, payload,
                                  unique_types, max_results)

    def _ext_query(self, subscription, event_ids, t_from, t_to,
                   source_t_from, source_t_to, payload, order,
                   unique_types, max_results):
        if self._order_by == common.OrderBy.TIMESTAMP:
            events = self._ext_query_events(t_from, t_to, order)

        elif self._order_by == common.OrderBy.SOURCE_TIMESTAMP:
            events = self._ext_query_events(source_t_from, source_t_to, order)

        else:
            raise ValueError('unsupported order by')

        events = (event for event in events if self._conditions.matches(event))

        events = _filter_events(events, subscription, event_ids, t_from,
                                t_to, source_t_from, source_t_to, payload,
                                unique_types, max_results)
        return list(events)

    def _ext_query_events(self, t_from, t_to, order):
        if not t_from:
            t_from = common.Timestamp(s=-(1 << 63), us=0)
        from_key = self._partition_id, t_from, 0
        from_key = encoder.encode_uint_timestamp_uint(from_key)

        if not t_to:
            t_to = common.Timestamp(s=(1 << 63) - 1, us=int(1e6))
        to_key = self._partition_id, t_to, ((1 << 64) - 1)
        to_key = encoder.encode_uint_timestamp_uint(to_key)

        with self._env.begin(db=self._data_db, buffers=True) as txn:
            cursor = txn.cursor()

            if order == common.Order.DESCENDING:
                start_key, stop_key = to_key, from_key

                if cursor.set_range(start_key):
                    more = cursor.prev()
                else:
                    more = cursor.last()

                while more and stop_key <= bytes(cursor.key()):
                    yield encoder.decode_event(cursor.value())
                    more = cursor.prev()

            elif order == common.Order.ASCENDING:
                start_key, stop_key = from_key, to_key

                more = cursor.set_range(start_key)

                while more and bytes(cursor.key()) < stop_key:
                    yield encoder.decode_event(cursor.value())
                    more = cursor.next()

            else:
                raise ValueError('unsupported order')

    def _ext_flush(self, changes, parent, now):
        with self._env.begin(db=self._count_db,
                             parent=parent,
                             buffers=True) as txn:
            value = txn.get(encoder.encode_uint(self._partition_id))
            entries_count = encoder.decode_uint(value) if value else 0

        with self._env.begin(db=self._data_db,
                             parent=parent,
                             write=True) as txn:
            stat = txn.stat(self._data_db)

            for timestamp, event in changes:
                key = self._partition_id, timestamp, event.event_id.instance
                txn.put(encoder.encode_uint_timestamp_uint(key),
                        encoder.encode_event(event))

            if not self._limit:
                return

            timestamp = common.Timestamp(s=-(1 << 63), us=0)
            start_key = self._partition_id, timestamp, 0
            start_key = encoder.encode_uint_timestamp_uint(start_key)
            stop_key = (self._partition_id + 1), timestamp, 0
            stop_key = encoder.encode_uint_timestamp_uint(stop_key)

            min_entries = self._limit.get('min_entries', 0)
            entries_count += len(changes)
            cursor = txn.cursor()
            more = cursor.set_range(start_key)

            if 'max_entries' in self._limit:
                max_entries = self._limit['max_entries']

                while (more and
                        entries_count > min_entries and
                        entries_count > max_entries and
                        bytes(cursor.key()) < stop_key):
                    more = cursor.delete()
                    entries_count -= 1

            if 'duration' in self._limit:
                duration = self._limit['duration']
                duration_key = self._partition_id, now.add(-duration), 0
                duration_key = encoder.encode_uint_timestamp_uint(duration_key)

                while (more and
                        entries_count > min_entries and
                        bytes(cursor.key()) < duration_key):
                    more = cursor.delete()
                    entries_count -= 1

            if 'size' in self._limit and stat['entries']:
                total_size = stat['psize'] * (stat['branch_pages'] +
                                              stat['leaf_pages'] +
                                              stat['overflow_pages'])
                entry_size = total_size / stat['entries']
                max_entries = int(self._limit['size'] / entry_size)

                while (more and
                        entries_count > min_entries and
                        entries_count > max_entries and
                        bytes(cursor.key()) < stop_key):
                    more = cursor.delete()
                    entries_count -= 1

        with self._env.begin(db=self._count_db,
                             parent=parent,
                             write=True) as txn:
            txn.put(encoder.encode_uint(self._partition_id),
                    encoder.encode_uint(entries_count))


def _filter_events(events, subscription, event_ids, t_from, t_to,
                   source_t_from, source_t_to, payload, unique_types,
                   max_results):
    for event in events:
        if max_results is not None and max_results <= 0:
            return

        if subscription and not subscription.matches(event.event_type):
            continue

        if event_ids is not None and event.event_id not in event_ids:
            continue

        if t_from is not None and event.timestamp < t_from:
            continue

        if t_to is not None and t_to < event.timestamp:
            continue

        if source_t_from is not None and (
                event.source_timestamp is None or
                event.source_timestamp < source_t_from):
            continue

        if source_t_to is not None and (
                event.source_timestamp is None or
                source_t_to < event.source_timestamp):
            continue

        if payload is not None and event.payload != payload:
            continue

        if unique_types is not None:
            if event.event_type in unique_types:
                continue
            unique_types.add(event.event_type)

        if max_results is not None:
            max_results -= 1

        yield event
