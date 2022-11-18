import asyncio
import collections
import itertools

import pytest

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import environment
import hat.event.server.backends.lmdb.conditions
import hat.event.server.backends.lmdb.ordereddb


class RefDb:

    def __init__(self):
        self._add_event_id_queue = collections.deque()
        self._remove_event_id_queue = collections.deque()

    @property
    def add_event_id_queue(self):
        return self._add_event_id_queue

    @property
    def remove_event_id_queue(self):
        return self._remove_event_id_queue

    def ext_add_event_ref(self, parent_txn, event_id, ref):
        assert isinstance(ref, common.OrderedEventRef)
        self._add_event_id_queue.append(event_id)

    def ext_remove_event_ref(self, parent_txn, event_id, ref):
        assert isinstance(ref, common.OrderedEventRef)
        self._remove_event_id_queue.append(event_id)


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'db'


@pytest.fixture
async def env(db_path):
    db_map_size = 1024 * 1024 * 1024
    env = await environment.create(db_path, db_map_size)

    try:
        yield env

    finally:
        await env.async_close()


@pytest.fixture
def create_event():
    session_count = itertools.count(1)
    instance_count = itertools.count(1)

    def create_event(event_type, with_source_timestamp, server_id=1):
        event_id = common.EventId(server_id,
                                  next(session_count),
                                  next(instance_count))
        t = common.now()
        event = common.Event(event_id=event_id,
                             event_type=event_type,
                             timestamp=t,
                             source_timestamp=(t if with_source_timestamp
                                               else None),
                             payload=None)
        return event

    return create_event


async def create_ordered_db(env, ref_db, subscription, conditions,
                            order_by, limit):
    return await env.execute(
        hat.event.server.backends.lmdb.ordereddb.ext_create, env, ref_db,
        subscription, conditions, order_by, limit)


async def flush(env, db):
    with env.ext_begin(write=True) as txn:
        await env.execute(db.create_ext_flush(), txn)


async def apply_limit(env, db, timestamp):
    return await env.execute(db.ext_apply_limit, timestamp)


async def query(db, subscription=None, server_id=None, event_ids=None,
                t_from=None, t_to=None, source_t_from=None,
                source_t_to=None, payload=None,
                order=common.Order.DESCENDING, unique_type=False,
                max_results=None):
    return list(await db.query(subscription=subscription,
                               server_id=server_id,
                               event_ids=event_ids,
                               t_from=t_from,
                               t_to=t_to,
                               source_t_from=source_t_from,
                               source_t_to=source_t_to,
                               payload=payload,
                               order=order,
                               unique_type=unique_type,
                               max_results=max_results))


async def test_create(env):
    subscription = common.Subscription([])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 common.OrderBy.TIMESTAMP, None)

    assert db.subscription == subscription
    assert db.order_by == common.OrderBy.TIMESTAMP
    assert db.partition_id == 1
    result = await query(db)
    assert result == []

    subscription2 = common.Subscription([('*',)])
    db2 = await create_ordered_db(env, ref_db, subscription2, conditions,
                                  common.OrderBy.TIMESTAMP, None)

    assert db2.subscription == subscription2
    assert db2.order_by == common.OrderBy.TIMESTAMP
    assert db2.partition_id == 2

    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 common.OrderBy.TIMESTAMP, None)

    assert db.partition_id == 1

    db3 = await create_ordered_db(env, ref_db, subscription, conditions,
                                  common.OrderBy.SOURCE_TIMESTAMP, None)

    assert db3.subscription == subscription
    assert db3.order_by == common.OrderBy.SOURCE_TIMESTAMP
    assert db3.partition_id == 3


@pytest.mark.parametrize('order_by', common.OrderBy)
async def test_add(env, create_event, order_by):
    subscription = common.Subscription([('a',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 order_by, None)

    event1 = create_event(('a',), True)
    event2 = create_event(('b',), True)
    event3 = create_event(('a',), False)
    event4 = create_event(('a',), True)

    db.add(event1)
    db.add(event2)

    result = await query(db)
    assert result == [event1]

    await flush(env, db)

    db.add(event3)
    db.add(event4)

    result = await query(db)
    if order_by == common.OrderBy.TIMESTAMP:
        assert result == [event4, event3, event1]
    elif order_by == common.OrderBy.SOURCE_TIMESTAMP:
        assert result == [event4, event1]


@pytest.mark.parametrize('order_by', common.OrderBy)
@pytest.mark.parametrize('order', common.Order)
async def test_query_max_results(env, create_event, order_by, order):
    subscription = common.Subscription([('a',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 order_by, None)

    event1 = create_event(('a',), True)
    event2 = create_event(('a',), True)
    event3 = create_event(('a',), True)

    result = await query(db, max_results=2, order=order)
    assert result == []

    db.add(event1)

    result = await query(db, max_results=2, order=order)
    assert result == [event1]

    await flush(env, db)

    result = await query(db, max_results=2, order=order)
    assert result == [event1]

    db.add(event2)

    result = await query(db, max_results=2, order=order)
    if order == common.Order.DESCENDING:
        assert result == [event2, event1]
    elif order == common.Order.ASCENDING:
        assert result == [event1, event2]

    db.add(event3)

    result = await query(db, max_results=2, order=order)
    if order == common.Order.DESCENDING:
        assert result == [event3, event2]
    elif order == common.Order.ASCENDING:
        assert result == [event1, event2]

    await flush(env, db)

    result = await query(db, max_results=2, order=order)
    if order == common.Order.DESCENDING:
        assert result == [event3, event2]
    elif order == common.Order.ASCENDING:
        assert result == [event1, event2]


@pytest.mark.parametrize('order_by', common.OrderBy)
@pytest.mark.parametrize('order', common.Order)
async def test_query_timestamps(env, create_event, order_by, order):
    subscription = common.Subscription([('a',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 order_by, None)

    event1 = create_event(('a',), True)
    await asyncio.sleep(0.001)
    event2 = create_event(('a',), True)
    await asyncio.sleep(0.001)
    event3 = create_event(('a',), True)
    await asyncio.sleep(0.001)
    event4 = create_event(('a',), True)
    await asyncio.sleep(0.001)

    db.add(event1)
    db.add(event2)
    db.add(event3)
    db.add(event4)

    for _ in range(2):
        result = await query(db, order=order)
        if order == common.Order.DESCENDING:
            assert result == [event4, event3, event2, event1]
        elif order == common.Order.ASCENDING:
            assert result == [event1, event2, event3, event4]

        result = await query(db, order=order,
                             t_from=event2.timestamp)
        if order == common.Order.DESCENDING:
            assert result == [event4, event3, event2]
        elif order == common.Order.ASCENDING:
            assert result == [event2, event3, event4]

        result = await query(db, order=order,
                             source_t_from=event2.source_timestamp)
        if order == common.Order.DESCENDING:
            assert result == [event4, event3, event2]
        elif order == common.Order.ASCENDING:
            assert result == [event2, event3, event4]

        result = await query(db, order=order,
                             t_to=event3.timestamp)
        if order == common.Order.DESCENDING:
            assert result == [event3, event2, event1]
        elif order == common.Order.ASCENDING:
            assert result == [event1, event2, event3]

        result = await query(db, order=order,
                             source_t_to=event3.source_timestamp)
        if order == common.Order.DESCENDING:
            assert result == [event3, event2, event1]
        elif order == common.Order.ASCENDING:
            assert result == [event1, event2, event3]

        result = await query(db, order=order,
                             t_from=event1.timestamp, t_to=event4.timestamp,
                             source_t_from=event1.source_timestamp,
                             source_t_to=event4.source_timestamp)
        if order == common.Order.DESCENDING:
            assert result == [event4, event3, event2, event1]
        elif order == common.Order.ASCENDING:
            assert result == [event1, event2, event3, event4]

        t = common.now()
        result = await query(db, order=order,
                             t_from=t, t_to=t,
                             source_t_from=t,
                             source_t_to=t)
        assert result == []

        await flush(env, db)


@pytest.mark.parametrize('order_by', common.OrderBy)
async def test_query_subscription(env, create_event, order_by):
    subscription = common.Subscription([('*',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 order_by, None)

    event1 = create_event(('a',), True)
    event2 = create_event(('b',), True)

    db.add(event1)
    db.add(event2)

    result = await query(db)
    assert result == [event2, event1]

    subscription = common.Subscription([('a',)])
    result = await query(db, subscription=subscription)
    assert result == [event1]

    subscription = common.Subscription([('b',)])
    result = await query(db, subscription=subscription)
    assert result == [event2]


@pytest.mark.parametrize('order_by', common.OrderBy)
async def test_query_server_id(env, create_event, order_by):
    subscription = common.Subscription([('*',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 order_by, None)

    event1 = create_event(('a',), True, server_id=123)
    event2 = create_event(('a', 'b'), True, server_id=123)
    event3 = create_event(('a',), True, server_id=456)
    event4 = create_event(('a', 'b'), True, server_id=456)

    db.add(event1)
    db.add(event2)
    db.add(event3)
    db.add(event4)

    result = await query(db)
    assert result == [event4, event3, event2, event1]

    result = await query(db, server_id=1)
    assert result == []

    result = await query(db, server_id=123)
    assert result == [event2, event1]

    result = await query(db, server_id=456)
    assert result == [event4, event3]


@pytest.mark.parametrize('order_by', common.OrderBy)
async def test_query_event_ids(env, create_event, order_by):
    subscription = common.Subscription([('*',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 order_by, None)

    event1 = create_event(('a',), True)
    event2 = create_event(('b',), True)

    db.add(event1)
    db.add(event2)

    result = await query(db)
    assert result == [event2, event1]

    result = await query(db, event_ids=[])
    assert result == []

    result = await query(db, event_ids=[event1.event_id])
    assert result == [event1]

    result = await query(db, event_ids=[event2.event_id])
    assert result == [event2]

    result = await query(db, event_ids=[event1.event_id, event2.event_id])
    assert result == [event2, event1]


@pytest.mark.parametrize('order_by', common.OrderBy)
async def test_query_payload(env, create_event, order_by):
    subscription = common.Subscription([('*',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 order_by, None)

    event1 = create_event(('a',), True)
    event2 = create_event(('b',), True)._replace(payload=common.EventPayload(
        common.EventPayloadType.JSON,
        data=123))

    db.add(event1)
    db.add(event2)

    result = await query(db)
    assert result == [event2, event1]

    result = await query(db, payload=common.EventPayload(
        common.EventPayloadType.JSON,
        data=123))
    assert result == [event2]

    result = await query(db, payload=common.EventPayload(
        common.EventPayloadType.JSON,
        data=321))
    assert result == []


@pytest.mark.parametrize('order_by', common.OrderBy)
async def test_query_unique_type(env, create_event, order_by):
    subscription = common.Subscription([('*',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 order_by, None)

    event1 = create_event(('a',), True)
    event2 = create_event(('b',), True)
    event3 = create_event(('a',), True)
    event4 = create_event(('b',), True)

    db.add(event1)
    db.add(event2)
    db.add(event3)
    db.add(event4)

    result = await query(db)
    assert result == [event4, event3, event2, event1]

    result = await query(db, unique_type=True)
    assert result == [event4, event3]


@pytest.mark.parametrize('order_by', common.OrderBy)
async def test_limit_max_entries(env, create_event, order_by):
    subscription = common.Subscription([('*',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    limit = {'max_entries': 3}
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 order_by, limit)

    for i in range(limit['max_entries'] * 2):
        db.add(create_event(('a',), True))
        await flush(env, db)
        await apply_limit(env, db, common.now())

        expected_len = (i + 1 if i < limit['max_entries']
                        else limit['max_entries'])
        result = await query(db)
        assert expected_len == len(result)


@pytest.mark.parametrize('order_by', common.OrderBy)
async def test_limit_min_entries(env, create_event, order_by):
    subscription = common.Subscription([('*',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    limit = {'min_entries': 5,
             'max_entries': 3}
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 order_by, limit)

    for i in range(limit['min_entries'] * 2):
        db.add(create_event(('a',), True))
        await flush(env, db)
        await apply_limit(env, db, common.now())

        expected_len = (i + 1 if i < limit['min_entries']
                        else limit['min_entries'])
        result = await query(db)
        assert expected_len == len(result)


@pytest.mark.parametrize('order_by', common.OrderBy)
async def test_limit_duration(env, create_event, order_by):
    subscription = common.Subscription([('*',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    limit = {'duration': 1}
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 order_by, limit)

    t1 = common.now()
    t2 = t1._replace(s=t1.s + 2 * limit['duration'])
    t3 = t2._replace(s=t2.s + 2 * limit['duration'])

    event1 = create_event(('a',), True)._replace(timestamp=t1,
                                                 source_timestamp=t1)
    event2 = create_event(('a',), True)._replace(timestamp=t2,
                                                 source_timestamp=t2)

    db.add(event1)
    db.add(event2)

    result = await query(db)
    assert result == [event2, event1]

    await flush(env, db)
    await apply_limit(env, db, t2)

    result = await query(db)
    assert result == [event2]

    await flush(env, db)
    await apply_limit(env, db, t3)

    result = await query(db)
    assert result == []


async def test_limit_size(env, create_event):
    subscription = common.Subscription([('*',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    limit = {'size': 100000}
    ref_db = RefDb()
    db = await create_ordered_db(env, ref_db, subscription, conditions,
                                 common.OrderBy.TIMESTAMP, limit)

    events_all = []

    for i in range(1000):
        event = create_event(('a',), True)
        db.add(event)
        events_all.append(event)
    await flush(env, db)
    await apply_limit(env, db, common.now())

    events_persisted = await query(db)
    assert events_persisted == events_all[::-1]

    # limit is exceeded -> less events are persisted than registered
    for i in range(200):
        event = create_event(('a',), True)
        db.add(event)
        events_all.append(event)
    await flush(env, db)
    await apply_limit(env, db, common.now())

    events_persisted = await query(db)
    assert len(events_persisted) < len(events_all)
    assert events_persisted == events_all[-len(events_persisted):][::-1]
    max_events_count = len(events_persisted)

    # after limit is reached, additional registration results with even less
    # events persisted, since events are of the same size -> db size increases
    # faster than linearly with number of events
    for i in range(200):
        event = create_event(('a',), True)
        db.add(event)
        events_all.append(event)
    await flush(env, db)
    await apply_limit(env, db, common.now())

    events_persisted = await query(db)
    assert len(events_persisted) <= max_events_count
    assert events_persisted == events_all[-len(events_persisted):][::-1]

    for i in range(200):
        event = create_event(('a',), True)
        db.add(event)
        events_all.append(event)
    await flush(env, db)
    await apply_limit(env, db, common.now())

    events_persisted = await query(db)
    assert len(events_persisted) <= max_events_count
    assert events_persisted == events_all[-len(events_persisted):][::-1]

    # db = await create_ordered_db(env, ref_db, subscription, conditions,
    #                              common.OrderBy.TIMESTAMP, limit)

    # events = await query(db)
    # assert events == events_persisted
