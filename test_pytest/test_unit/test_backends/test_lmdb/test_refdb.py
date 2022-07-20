import itertools

import lmdb
import pytest

from hat import aio
from hat.event.server.backends.lmdb import common
import hat.event.server.backends.lmdb.conditions
import hat.event.server.backends.lmdb.latestdb
import hat.event.server.backends.lmdb.ordereddb
import hat.event.server.backends.lmdb.refdb


db_map_size = 1024 * 1024 * 1024
db_max_dbs = 32


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'db'


@pytest.fixture
async def executor():
    return aio.create_executor(1)


@pytest.fixture
async def env(executor, db_path):
    env = await executor(lmdb.Environment, str(db_path),
                         map_size=db_map_size, subdir=False,
                         max_dbs=db_max_dbs)
    try:
        yield env
    finally:
        await executor(env.close)


@pytest.fixture
def flush(executor, env):

    async def flush(dbs):
        txn = await executor(env.begin, write=True)
        ctx = common.FlushContext(txn)
        try:
            for db in dbs:
                await executor(db.create_ext_flush(), ctx)
        finally:
            await executor(txn.commit)

    return flush


@pytest.fixture
def create_event():
    instance_cntrs = {}

    def create_event(session):
        nonlocal instance_cntrs
        if session not in instance_cntrs:
            instance_cntrs[session] = itertools.count(1)
        instance = next(instance_cntrs[session])
        event_id = common.EventId(1, session, instance)
        event = common.Event(event_id=event_id,
                             event_type=('a', str(instance)),
                             timestamp=common.now(),
                             source_timestamp=None,
                             payload=common.EventPayload(
                                type=common.EventPayloadType.JSON,
                                data=instance))
        return event

    return create_event


async def test_create(executor, env):
    db = await hat.event.server.backends.lmdb.refdb.create(
        executor=executor,
        env=env)
    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == []


async def test_query(executor, env, create_event, flush):
    db = await hat.event.server.backends.lmdb.refdb.create(
        executor=executor,
        env=env)

    subscription = common.Subscription([('*',)])
    conditions = hat.event.server.backends.lmdb.conditions.Conditions([])
    ordereddb = await hat.event.server.backends.lmdb.ordereddb.create(
        executor=executor,
        env=env,
        subscription=subscription,
        conditions=conditions,
        order_by=common.OrderBy.TIMESTAMP,
        limit=None)

    events1 = [create_event(session=1) for i in range(10)]

    events2 = [create_event(session=2) for i in range(10)]

    for e in events1:
        ordereddb.add(e)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == []

    await flush([ordereddb, db])

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [events1]

    result = [i async for i in db.query(common.EventId(1, 0, 5))]
    assert result == [events1]

    result = [i async for i in db.query(common.EventId(1, 1, 3))]
    assert result == [events1[3:]]

    result = [i async for i in db.query(common.EventId(1, 2, 0))]
    assert result == []

    result = [i async for i in db.query(common.EventId(2, 0, 0))]
    assert result == []

    for e in events2:
        ordereddb.add(e)

    await flush([ordereddb, db])

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [events1, events2]

    result = [i async for i in db.query(common.EventId(1, 1, 7))]
    assert result == [events1[7:], events2]

    result = [i async for i in db.query(common.EventId(1, 2, 0))]
    assert result == [events2]

    result = [i async for i in db.query(common.EventId(1, 2, 6))]
    assert result == [events2[6:]]

    latestdb = await hat.event.server.backends.lmdb.latestdb.create(
        executor=executor,
        env=env,
        subscription=subscription,
        conditions=conditions)

    events3 = [create_event(session=3) for i in range(10)]
    for e in events3:
        latestdb.add(e)
    await flush([latestdb, ordereddb, db])

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [events1, events2, events3]

    for e in events3:
        ordereddb.add(e)
    await flush([latestdb, ordereddb, db])

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [events1, events2, events3]

    events4 = [create_event(session=3) for i in range(10)]
    for e in events4:
        latestdb.add(e)
        ordereddb.add(e)
    await flush([latestdb, ordereddb, db])

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [events1, events2, events3 + events4]

    result = [i async for i in db.query(common.EventId(1, 3, 5))]
    assert result == [events3[5:] + events4]

    result = [i async for i in db.query(common.EventId(1, 3, 19))]
    assert len(result) == 1
    assert result == [events4[-1:]]

    result = [i async for i in db.query(common.EventId(1, 3, 20))]
    assert not result

    db = await hat.event.server.backends.lmdb.refdb.create(
        executor=executor,
        env=env)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [events1, events2, events3 + events4]
