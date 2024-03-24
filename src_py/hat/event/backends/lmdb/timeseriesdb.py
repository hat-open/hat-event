from collections.abc import Iterable
import collections
import itertools
import typing

import lmdb

from hat.event.backends.lmdb import common
from hat.event.backends.lmdb import environment
from hat.event.backends.lmdb.conditions import Conditions


Changes: typing.TypeAlias = dict[common.PartitionId,
                                 collections.deque[tuple[common.Timestamp,
                                                         common.Event]]]


class Limit(typing.NamedTuple):
    min_entries: int | None = None
    max_entries: int | None = None
    duration: float | None = None
    size: int | None = None


class Partition(typing.NamedTuple):
    order_by: common.OrderBy
    subscription: common.Subscription
    limit: Limit | None


def ext_create(env: environment.Environment,
               txn: lmdb.Transaction,
               conditions: Conditions,
               partitions: Iterable[Partition],
               max_results: int = 4096
               ) -> 'TimeseriesDb':
    db = TimeseriesDb()
    db._env = env
    db._conditions = conditions
    db._max_results = max_results
    db._changes = collections.defaultdict(collections.deque)

    # depending on dict order
    db._partitions = dict(_ext_init_partitions(env, txn, partitions))

    return db


class TimeseriesDb:

    def add(self,
            event: common.Event
            ) -> Iterable[common.EventRef]:
        for partition_id, partition in self._partitions.items():
            if not partition.subscription.matches(event.type):
                continue

            if partition.order_by == common.OrderBy.TIMESTAMP:
                timestamp = event.timestamp

            elif partition.order_by == common.OrderBy.SOURCE_TIMESTAMP:
                if event.source_timestamp is None:
                    continue

                timestamp = event.source_timestamp

            else:
                raise ValueError('unsupported order by')

            self._changes[partition_id].append((timestamp, event))

            yield common.TimeseriesEventRef(
                (partition_id, timestamp, event.id))

    async def query(self,
                    params: common.QueryTimeseriesParams
                    ) -> common.QueryResult:
        subscription = (common.create_subscription(params.event_types)
                        if params.event_types is not None else None)

        max_results = (params.max_results
                       if params.max_results is not None and
                       params.max_results < self._max_results
                       else self._max_results)

        for partition_id, partition in self._partitions.items():
            if partition.order_by != params.order_by:
                continue

            if (subscription and
                    subscription.isdisjoint(partition.subscription)):
                continue

            return await self._query_partition(partition_id, params,
                                               subscription, max_results)

        return common.QueryResult(events=[],
                                  more_follows=False)

    def create_changes(self) -> Changes:
        changes, self._changes = (self._changes,
                                  collections.defaultdict(collections.deque))
        return changes

    def ext_write(self,
                  txn: lmdb.Transaction,
                  changes: Changes):
        data = (((partition_id, timestamp, event.id), event)
                for partition_id, partition_changes in changes.items()
                for timestamp, event in partition_changes)
        self._env.ext_write(txn, common.DbType.TIMESERIES_DATA, data)

        counts = ((partition_id, len(partition_changes))
                  for partition_id, partition_changes in changes.items())
        _ext_inc_partition_count(self._env, txn, counts)

    def ext_cleanup(self,
                    txn: lmdb.Transaction,
                    now: common.Timestamp,
                    max_results: int | None = None,
                    ) -> collections.deque[tuple[common.EventId,
                                                 common.EventRef]]:
        result = collections.deque()

        for partition_id, partition in self._partitions.items():
            if not partition.limit:
                continue

            partition_max_results = (max_results - len(result)
                                     if max_results is not None else None)
            if partition_max_results is not None and partition_max_results < 1:
                break

            result.extend(_ext_cleanup_partition(self._env, txn, now,
                                                 partition_id, partition.limit,
                                                 partition_max_results))

        return result

    async def _query_partition(self, partition_id, params, subscription,
                               max_results):
        events = collections.deque()
        changes = self._changes

        filter = _Filter(subscription=subscription,
                         t_from=params.t_from,
                         t_to=params.t_to,
                         source_t_from=params.source_t_from,
                         source_t_to=params.source_t_to,
                         max_results=max_results + 1,
                         last_event_id=params.last_event_id)

        if params.order == common.Order.DESCENDING:
            events.extend(_query_partition_changes(
                changes[partition_id], params, filter))

            if not filter.done:
                events.extend(await self._env.execute(
                    _ext_query_partition_events, self._env, self._conditions,
                    partition_id, params, filter))

        elif params.order == common.Order.ASCENDING:
            events.extend(await self._env.execute(
                _ext_query_partition_events, self._env, self._conditions,
                partition_id, params, filter))

            if not filter.done:
                events.extend(_query_partition_changes(
                    changes[partition_id], params, filter))

        else:
            raise ValueError('unsupported order')

        more_follows = len(events) > max_results
        while len(events) > max_results:
            events.pop()

        return common.QueryResult(events=events,
                                  more_follows=more_follows)


def _ext_init_partitions(env, txn, partitions):
    db_data = dict(env.ext_read(txn, common.DbType.TIMESERIES_PARTITION))
    next_partition_ids = itertools.count(max(db_data.keys(), default=0) + 1)

    for partition in partitions:
        event_types = sorted(partition.subscription.get_query_types())
        partition_data = {'order': partition.order_by.value,
                          'subscriptions': [list(i) for i in event_types]}

        for partition_id, i in db_data.items():
            if i == partition_data:
                break

        else:
            partition_id = next(next_partition_ids)
            db_data[partition_id] = partition_data
            env.ext_write(txn, common.DbType.TIMESERIES_PARTITION,
                          [(partition_id, partition_data)])

        yield partition_id, partition


def _ext_query_partition_events(env, conditions, partition_id, params,
                                filter):
    if params.order_by == common.OrderBy.TIMESTAMP:
        events = _ext_query_partition_events_range(
            env, partition_id, params.t_from, params.t_to, params.order)

    elif params.order_by == common.OrderBy.SOURCE_TIMESTAMP:
        events = _ext_query_partition_events_range(
            env, partition_id, params.source_t_from, params.source_t_to,
            params.order)

    else:
        raise ValueError('unsupported order by')

    events = (event for event in events if conditions.matches(event))
    events = filter.process(events)
    return collections.deque(events)


def _ext_query_partition_events_range(env, partition_id, t_from, t_to, order):
    db_def = common.db_defs[common.DbType.TIMESERIES_DATA]

    if not t_from:
        t_from = common.min_timestamp

    from_key = partition_id, t_from, common.EventId(0, 0, 0)
    encoded_from_key = db_def.encode_key(from_key)

    if not t_to:
        t_to = common.max_timestamp

    to_key = (partition_id,
              t_to,
              common.EventId((1 << 64) - 1, (1 << 64) - 1, (1 << 64) - 1))
    encoded_to_key = db_def.encode_key(to_key)

    with env.ext_begin() as txn:
        with env.ext_cursor(txn, common.DbType.TIMESERIES_DATA) as cursor:
            if order == common.Order.DESCENDING:
                encoded_start_key, encoded_stop_key = (encoded_to_key,
                                                       encoded_from_key)

                if cursor.set_range(encoded_start_key):
                    more = cursor.prev()
                else:
                    more = cursor.last()

                while more and encoded_stop_key <= bytes(cursor.key()):
                    yield db_def.decode_value(cursor.value())
                    more = cursor.prev()

            elif order == common.Order.ASCENDING:
                encoded_start_key, encoded_stop_key = (encoded_from_key,
                                                       encoded_to_key)

                more = cursor.set_range(encoded_start_key)

                while more and bytes(cursor.key()) <= encoded_stop_key:
                    yield db_def.decode_value(cursor.value())
                    more = cursor.next()

            else:
                raise ValueError('unsupported order')


def _ext_get_partition_count(env, txn, partition_id):
    db_def = common.db_defs[common.DbType.TIMESERIES_COUNT]

    with env.ext_cursor(txn, common.DbType.TIMESERIES_COUNT) as cursor:
        encoded_key = db_def.encode_key(partition_id)
        encoded_value = cursor.get(encoded_key)

        return db_def.decode_value(encoded_value) if encoded_value else 0


def _ext_set_partition_count(env, txn, partition_id, count):
    env.ext_write(txn, common.DbType.TIMESERIES_COUNT, [(partition_id, count)])


def _ext_inc_partition_count(env, txn, partition_counts):
    db_def = common.db_defs[common.DbType.TIMESERIES_COUNT]

    with env.ext_cursor(txn, common.DbType.TIMESERIES_COUNT) as cursor:
        for partition_id, count in partition_counts:
            encoded_key = db_def.encode_key(partition_id)
            encoded_value = cursor.get(encoded_key)

            value = db_def.decode_value(encoded_value) if encoded_value else 0
            inc_value = value + count

            encoded_value = db_def.encode_value(inc_value)
            cursor.put(encoded_key, encoded_value)


def _ext_cleanup_partition(env, txn, now, partition_id, limit, max_results):
    db_def = common.db_defs[common.DbType.TIMESERIES_DATA]

    timestamp = common.min_timestamp
    start_key = (partition_id,
                 timestamp,
                 common.EventId(0, 0, 0))
    stop_key = ((partition_id + 1),
                timestamp,
                common.EventId(0, 0, 0))

    encoded_start_key = db_def.encode_key(start_key)
    encoded_stop_key = db_def.encode_key(stop_key)

    min_entries = limit.min_entries or 0
    max_entries = None
    encoded_duration_key = None

    if limit.size is not None:
        stat = env.ext_stat(txn, common.DbType.TIMESERIES_DATA)

        if stat['entries']:
            total_size = stat['psize'] * (stat['branch_pages'] +
                                          stat['leaf_pages'] +
                                          stat['overflow_pages'])
            entry_size = total_size / stat['entries']
            max_entries = int(limit.size / entry_size)

    if limit.max_entries is not None:
        max_entries = (limit.max_entries if max_entries is None
                       else min(max_entries, limit.max_entries))

    if limit.duration is not None:
        duration_key = (partition_id,
                        now.add(-limit.duration),
                        common.EventId(0, 0, 0))
        encoded_duration_key = db_def.encode_key(duration_key)

    result_count = 0
    entries_count = _ext_get_partition_count(env, txn, partition_id)

    with env.ext_cursor(txn, common.DbType.TIMESERIES_DATA) as cursor:
        more = cursor.set_range(encoded_start_key)
        while more:
            if max_results is not None and result_count >= max_results:
                break

            if entries_count - result_count <= min_entries:
                break

            encoded_key = bytes(cursor.key())
            if encoded_key >= encoded_stop_key:
                break

            if ((max_entries is None or
                 entries_count - result_count <= max_entries) and
                (encoded_duration_key is None or
                 encoded_key >= encoded_duration_key)):
                break

            key = db_def.decode_key(encoded_key)
            event_id = key[2]

            more = cursor.delete()
            result_count += 1

            yield event_id, common.TimeseriesEventRef(key)

    if result_count > 0:
        _ext_set_partition_count(env, txn, partition_id,
                                 entries_count - result_count)


def _query_partition_changes(changes, params, filter):
    if params.order == common.Order.DESCENDING:
        events = (event for _, event in reversed(changes))

        if (params.order_by == common.OrderBy.TIMESTAMP and
                params.t_to is not None):
            events = itertools.dropwhile(
                lambda i: params.t_to < i.timestamp,
                events)

        elif (params.order_by == common.OrderBy.SOURCE_TIMESTAMP and
                params.source_t_to is not None):
            events = itertools.dropwhile(
                lambda i: params.source_t_to < i.source_timestamp,
                events)

        if (params.order_by == common.OrderBy.TIMESTAMP and
                params.t_from is not None):
            events = itertools.takewhile(
                lambda i: params.t_from <= i.timestamp,
                events)

        elif (params.order_by == common.OrderBy.SOURCE_TIMESTAMP and
                params.source_t_from is not None):
            events = itertools.takewhile(
                lambda i: params.source_t_from <= i.source_timestamp,
                events)

    elif params.order == common.Order.ASCENDING:
        events = (event for _, event in changes)

        if (params.order_by == common.OrderBy.TIMESTAMP and
                params.t_from is not None):
            events = itertools.dropwhile(
                lambda i: i.timestamp < params.t_from,
                events)

        elif (params.order_by == common.OrderBy.SOURCE_TIMESTAMP and
                params.source_t_from is not None):
            events = itertools.dropwhile(
                lambda i: i.source_timestamp < params.source_t_from,
                events)

        if (params.order_by == common.OrderBy.TIMESTAMP and
                params.t_to is not None):
            events = itertools.takewhile(
                lambda i: i.timestamp <= params.t_to,
                events)

        elif (params.order_by == common.OrderBy.SOURCE_TIMESTAMP and
                params.source_t_to is not None):
            events = itertools.takewhile(
                lambda i: i.source_timestamp <= params.source_t_to,
                events)

    else:
        raise ValueError('unsupported order')

    return filter.process(events)


class _Filter:

    def __init__(self,
                 subscription: common.Subscription,
                 t_from: common.Timestamp | None,
                 t_to: common.Timestamp | None,
                 source_t_from: common.Timestamp | None,
                 source_t_to: common.Timestamp | None,
                 max_results: int,
                 last_event_id: common.EventId | None):
        self._subscription = subscription
        self._t_from = t_from
        self._t_to = t_to
        self._source_t_from = source_t_from
        self._source_t_to = source_t_to
        self._max_results = max_results
        self._last_event_id = last_event_id

    @property
    def done(self):
        return self._max_results < 1

    def process(self, events: Iterable[common.Event]):
        for event in events:
            if self._max_results < 1:
                return

            if self._last_event_id:
                if event.id == self._last_event_id:
                    self._last_event_id = None

                continue

            if self._t_from is not None and event.timestamp < self._t_from:
                continue

            if self._t_to is not None and self._t_to < event.timestamp:
                continue

            if self._source_t_from is not None and (
                    event.source_timestamp is None or
                    event.source_timestamp < self._source_t_from):
                continue

            if self._source_t_to is not None and (
                    event.source_timestamp is None or
                    self._source_t_to < event.source_timestamp):
                continue

            if (self._subscription and
                    not self._subscription.matches(event.type)):
                continue

            self._max_results -= 1
            yield event
