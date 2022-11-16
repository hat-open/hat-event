import collections
import itertools

import pytest

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import environment
import hat.event.server.backends.lmdb.conditions
import hat.event.server.backends.lmdb.latestdb


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
        assert isinstance(ref, common.LatestEventRef)
        self._add_event_id_queue.append(event_id)

    def ext_remove_event_ref(self, parent_txn, event_id, ref):
        assert isinstance(ref, common.LatestEventRef)
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

    def create_event(event_type, payload):
        event_id = common.EventId(1, next(session_count), next(instance_count))
        event = common.Event(event_id=event_id,
                             event_type=event_type,
                             timestamp=common.now(),
                             source_timestamp=None,
                             payload=payload)
        return event

    return create_event


async def create_latest_db(env, ref_db, subscription, conditions):
    return await env.execute(
        hat.event.server.backends.lmdb.latestdb.ext_create, env, ref_db,
        subscription, conditions)


async def flush(env, db):
    await env.execute(db.create_ext_flush(), None, common.now())


async def test_create(env):
    subscription = common.Subscription([])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_latest_db(env, ref_db, subscription, conditions)

    assert db.subscription == subscription
    result = list(db.query(None))
    assert result == []


async def test_add(env, create_event):
    subscription = common.Subscription([('*',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_latest_db(env, ref_db, subscription, conditions)

    result = list(db.query(None))
    assert result == []

    event1 = create_event(('a',), None)
    db.add(event1)

    result = set(db.query(None))
    assert result == {event1}

    event2 = create_event(('b',), None)
    db.add(event2)

    result = set(db.query(None))
    assert result == {event1, event2}

    event3 = create_event(('a',), None)
    db.add(event3)

    result = set(db.query(None))
    assert result == {event2, event3}

    await flush(env, db)

    assert sorted(ref_db.add_event_id_queue) == [event2.event_id,
                                                 event3.event_id]
    assert sorted(ref_db.remove_event_id_queue) == []

    db = await create_latest_db(env, ref_db, subscription, conditions)
    ref_db.add_event_id_queue.clear()
    ref_db.remove_event_id_queue.clear()

    result = set(db.query(None))
    assert result == {event2, event3}

    event4 = create_event(('a',), None)
    db.add(event4)

    await flush(env, db)

    assert sorted(ref_db.add_event_id_queue) == [event4.event_id]
    assert sorted(ref_db.remove_event_id_queue) == [event3.event_id]


async def test_query(env, create_event):
    subscription = common.Subscription([('a',), ('b',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_latest_db(env, ref_db, subscription, conditions)

    event1 = create_event(('a',), None)
    event2 = create_event(('b',), None)
    event3 = create_event(('c',), None)

    db.add(event1)
    db.add(event2)
    db.add(event3)

    result = set(db.query(None))
    assert result == {event1, event2}

    result = set(db.query([('a',), ('b',), ('c',)]))
    assert result == {event1, event2}

    result = set(db.query([('*',)]))
    assert result == {event1, event2}

    result = set(db.query([('a',)]))
    assert result == {event1}

    result = set(db.query([('b',)]))
    assert result == {event2}

    result = set(db.query([('c',)]))
    assert result == set()


async def test_subscription_change(env, create_event):
    subscription1 = common.Subscription([('a',)])
    subscription2 = common.Subscription([('b',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ref_db = RefDb()
    db = await create_latest_db(env, ref_db, subscription1, conditions)

    event = create_event(('a',), None)
    db.add(event)

    await flush(env, db)

    db = await create_latest_db(env, ref_db, subscription1, conditions)

    result = set(db.query(None))
    assert result == {event}

    db = await create_latest_db(env, ref_db, subscription2, conditions)

    result = set(db.query(None))
    assert result == set()


async def test_conditions_change(env, create_event):
    subscription = common.Subscription([('*',)])
    conditions1 = hat.event.server.backends.lmdb.conditions.Conditions([])
    conditions2 = hat.event.server.backends.lmdb.conditions.Conditions([{
        'subscriptions': [['*']],
        'condition': {'type': 'json'}}])
    ref_db = RefDb()
    db = await create_latest_db(env, ref_db, subscription, conditions1)

    event = create_event(('a',), None)
    db.add(event)

    await flush(env, db)

    db = await create_latest_db(env, ref_db, subscription, conditions1)

    result = set(db.query(None))
    assert result == {event}

    db = await create_latest_db(env, ref_db, subscription, conditions2)

    result = set(db.query(None))
    assert result == set()
