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


Changes = typing.Iterable[typing.Tuple[common.Timestamp, common.Event]]


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

    db._data_db = common.ext_open_db(env, common.DbType.ORDERED_DATA)
    db._count_db = common.ext_open_db(env, common.DbType.ORDERED_COUNT)

    db._partition_id = None
    last_partition_id = 0
    partition_data = {
        'order': order_by.value,
        'subscriptions': [list(i)
                          for i in sorted(subscription.get_query_types())]}

    partition_db = common.ext_open_db(env, common.DbType.ORDERED_PARTITION)

    with env.begin(db=partition_db, buffers=True) as txn:
        for encoded_key, encoded_value in txn.cursor():
            key = encoder.decode_ordered_partition_db_key(encoded_key)
            value = encoder.decode_ordered_partition_db_value(encoded_value)

            last_partition_id = key
            if value == partition_data:
                db._partition_id = last_partition_id
                break

    if db._partition_id is None:
        db._partition_id = last_partition_id + 1

        with env.begin(db=partition_db, write=True) as txn:
            key = db._partition_id
            value = partition_data

            encoded_key = encoder.encode_ordered_partition_db_key(key)
            encoded_value = encoder.encode_ordered_partition_db_value(value)

            txn.put(encoded_key, encoded_value)

    return db


class OrderedDb(common.Flushable):

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
        from_key = (self._partition_id,
                    t_from,
                    common.EventId(0, 0, 0))
        encoded_from_key = encoder.encode_ordered_data_db_key(from_key)

        if not t_to:
            t_to = common.Timestamp(s=(1 << 63) - 1, us=int(1e6))
        to_key = (self._partition_id,
                  t_to,
                  common.EventId((1 << 64) - 1, (1 << 64) - 1, (1 << 64) - 1))
        encoded_to_key = encoder.encode_ordered_data_db_key(to_key)

        with self._env.begin(db=self._data_db, buffers=True) as txn:
            cursor = txn.cursor()

            if order == common.Order.DESCENDING:
                encoded_start_key, encoded_stop_key = (encoded_to_key,
                                                       encoded_from_key)

                if cursor.set_range(encoded_start_key):
                    more = cursor.prev()
                else:
                    more = cursor.last()

                while more and encoded_stop_key <= bytes(cursor.key()):
                    yield encoder.decode_ordered_data_db_value(cursor.value())
                    more = cursor.prev()

            elif order == common.Order.ASCENDING:
                encoded_start_key, encoded_stop_key = (encoded_from_key,
                                                       encoded_to_key)

                more = cursor.set_range(encoded_start_key)

                while more and bytes(cursor.key()) < encoded_stop_key:
                    yield encoder.decode_ordered_data_db_value(cursor.value())
                    more = cursor.next()

            else:
                raise ValueError('unsupported order')

    def _ext_flush(self, changes, ctx):
        with self._env.begin(db=self._count_db,
                             parent=ctx.transaction,
                             buffers=True) as txn:
            key = self._partition_id
            encoded_key = encoder.encode_ordered_count_db_key(key)

            encoded_value = txn.get(encoded_key)
            value = (encoder.decode_ordered_count_db_value(encoded_value)
                     if encoded_value else 0)

            entries_count = value

        with self._env.begin(db=self._data_db,
                             parent=ctx.transaction,
                             write=True) as txn:
            stat = txn.stat(self._data_db)

            for timestamp, event in changes:
                key = self._partition_id, timestamp, event.event_id
                encoded_key = encoder.encode_ordered_data_db_key(key)

                value = event
                encoded_value = encoder.encode_ordered_data_db_value(value)

                txn.put(encoded_key, encoded_value)

                event_ref = common.OrderedEventRef(key)
                ctx.add_event_ref(event, event_ref)

            if not self._limit:
                return

            timestamp = common.Timestamp(s=-(1 << 63), us=0)
            start_key = (self._partition_id,
                         timestamp,
                         common.EventId(0, 0, 0))
            stop_key = ((self._partition_id + 1),
                        timestamp,
                        common.EventId(0, 0, 0))

            encoded_start_key = encoder.encode_ordered_data_db_key(start_key)
            encoded_stop_key = encoder.encode_ordered_data_db_key(stop_key)

            min_entries = self._limit.get('min_entries', 0)
            entries_count += len(changes)
            cursor = txn.cursor()
            more = cursor.set_range(encoded_start_key)

            if 'max_entries' in self._limit:
                max_entries = self._limit['max_entries']

                while (more and
                        entries_count > min_entries and
                        entries_count > max_entries and
                        bytes(cursor.key()) < encoded_stop_key):
                    encoded_key = cursor.key()
                    encoded_value = cursor.value()

                    key = encoder.decode_ordered_data_db_key(encoded_key)
                    value = encoder.decode_ordered_data_db_value(encoded_value)

                    more = cursor.delete()
                    entries_count -= 1

                    event_ref = common.OrderedEventRef(key)
                    ctx.remove_event_ref(value.event_id, event_ref)

            if 'duration' in self._limit:
                duration = self._limit['duration']
                duration_key = (self._partition_id,
                                ctx.timestamp.add(-duration),
                                common.EventId(0, 0, 0))
                encoded_duration_key = encoder.encode_ordered_data_db_key(
                    duration_key)

                while (more and
                        entries_count > min_entries and
                        bytes(cursor.key()) < encoded_duration_key):
                    encoded_key = cursor.key()
                    encoded_value = cursor.value()

                    key = encoder.decode_ordered_data_db_key(encoded_key)
                    value = encoder.decode_ordered_data_db_value(encoded_value)

                    more = cursor.delete()
                    entries_count -= 1

                    event_ref = common.OrderedEventRef(key)
                    ctx.remove_event_ref(value.event_id, event_ref)

            if 'size' in self._limit and stat['entries']:
                total_size = stat['psize'] * (stat['branch_pages'] +
                                              stat['leaf_pages'] +
                                              stat['overflow_pages'])
                entry_size = total_size / stat['entries']
                max_entries = int(self._limit['size'] / entry_size)

                while (more and
                        entries_count > min_entries and
                        entries_count > max_entries and
                        bytes(cursor.key()) < encoded_stop_key):
                    encoded_key = cursor.key()
                    encoded_value = cursor.value()

                    key = encoder.decode_ordered_data_db_key(encoded_key)
                    value = encoder.decode_ordered_data_db_value(encoded_value)

                    more = cursor.delete()
                    entries_count -= 1

                    event_ref = common.OrderedEventRef(key)
                    ctx.remove_event_ref(value.event_id, event_ref)

        with self._env.begin(db=self._count_db,
                             parent=ctx.transaction,
                             write=True) as txn:
            key = self._partition_id
            value = entries_count

            encoded_key = encoder.encode_ordered_count_db_key(key)
            encoded_value = encoder.encode_ordered_count_db_value(value)

            txn.put(encoded_key, encoded_value)


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
