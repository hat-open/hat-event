import asyncio

import pytest

from hat.event.backends.lmdb import common
from hat.event.backends.lmdb import environment
from hat.event.backends.lmdb.conditions import Conditions
from hat.event.backends.lmdb.timeseriesdb import Limit, Partition
import hat.event.backends.lmdb.timeseriesdb


default_partitions = [
    Partition(order_by=common.OrderBy.TIMESTAMP,
              subscription=common.create_subscription([('*', )]),
              limit=None),
    Partition(order_by=common.OrderBy.SOURCE_TIMESTAMP,
              subscription=common.create_subscription([('*', )]),
              limit=None)]


async def create_timeseries_db(env,
                               conditions=Conditions([]),
                               partitions=default_partitions,
                               max_results=4096,
                               event_type_cache_size=256 * 1024):

    def ext_create():
        with env.ext_begin(write=True) as txn:
            return hat.event.backends.lmdb.timeseriesdb.ext_create(
                env=env,
                txn=txn,
                conditions=conditions,
                partitions=partitions,
                max_results=max_results,
                event_type_cache_size=event_type_cache_size)

    return await env.execute(ext_create)


def add_timeseries_db(db, event):
    return list(db.add(event))


async def write_timeseries_db(env, db):

    def ext_write(changes):
        with env.ext_begin(write=True) as txn:
            db.ext_write(txn, changes)

    await env.execute(ext_write, db.create_changes())


async def cleanup_timeseries_db(env, db, now=None, max_results=None):

    def ext_cleanup(now):
        with env.ext_begin(write=True) as txn:
            db.ext_cleanup(txn, now, max_results)

    return await env.execute(ext_cleanup, now or common.now())


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'db'


async def test_create(db_path):
    env = await environment.create(db_path)

    db = await create_timeseries_db(env)
    assert db

    await env.async_close()


@pytest.mark.parametrize('events_count', [1, 5])
async def test_add(events_count, db_path):
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=common.now(),
                           payload=None)
              for i in range(events_count)]

    env = await environment.create(db_path)
    db = await create_timeseries_db(env, max_results=events_count)

    result = await db.query(common.QueryTimeseriesParams())
    assert list(result.events) == []

    for event in events:
        result = add_timeseries_db(db, event)
        assert len(result) == 2

    result = await db.query(
        common.QueryTimeseriesParams(order=common.Order.ASCENDING))
    assert list(result.events) == events
    assert result.more_follows is False

    result = await db.query(
        common.QueryTimeseriesParams(order=common.Order.DESCENDING))
    assert list(result.events) == list(reversed(events))
    assert result.more_follows is False

    await write_timeseries_db(env, db)
    await env.async_close()

    env = await environment.create(db_path)
    db = await create_timeseries_db(env, max_results=events_count)

    result = await db.query(
        common.QueryTimeseriesParams(order=common.Order.ASCENDING))
    assert list(result.events) == events
    assert result.more_follows is False

    result = await db.query(
        common.QueryTimeseriesParams(order=common.Order.DESCENDING))
    assert list(result.events) == list(reversed(events))
    assert result.more_follows is False

    await env.async_close()


@pytest.mark.parametrize('order', common.Order)
@pytest.mark.parametrize('order_by', common.OrderBy)
async def test_query_max_results(order, order_by, db_path):
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', ),
                           timestamp=common.now(),
                           source_timestamp=common.now(),
                           payload=None)
              for i in range(4)]

    env = await environment.create(db_path)
    db = await create_timeseries_db(env, max_results=len(events) - 1)

    query_params = common.QueryTimeseriesParams(order=order,
                                                order_by=order_by)

    result = await db.query(query_params._replace(max_results=2))
    assert result.more_follows is False
    assert list(result.events) == []

    add_timeseries_db(db, events[0])

    result = await db.query(query_params._replace(max_results=2))
    assert result.more_follows is False
    assert list(result.events) == [events[0]]

    await write_timeseries_db(env, db)

    result = await db.query(query_params._replace(max_results=2))
    assert result.more_follows is False
    assert list(result.events) == [events[0]]

    add_timeseries_db(db, events[1])

    result = await db.query(query_params._replace(max_results=2))
    assert result.more_follows is False
    if order == common.Order.DESCENDING:
        assert list(result.events) == [events[1], events[0]]
    elif order == common.Order.ASCENDING:
        assert list(result.events) == [events[0], events[1]]

    add_timeseries_db(db, events[2])

    result = await db.query(query_params._replace(max_results=2))
    assert result.more_follows is True
    if order == common.Order.DESCENDING:
        assert list(result.events) == [events[2], events[1]]
    elif order == common.Order.ASCENDING:
        assert list(result.events) == [events[0], events[1]]

    await write_timeseries_db(env, db)

    result = await db.query(query_params._replace(max_results=2))
    assert result.more_follows is True
    if order == common.Order.DESCENDING:
        assert list(result.events) == [events[2], events[1]]
    elif order == common.Order.ASCENDING:
        assert list(result.events) == [events[0], events[1]]

    result = await db.query(query_params._replace(max_results=None))
    assert result.more_follows is False
    if order == common.Order.DESCENDING:
        assert list(result.events) == [events[2], events[1], events[0]]
    elif order == common.Order.ASCENDING:
        assert list(result.events) == [events[0], events[1], events[2]]

    add_timeseries_db(db, events[3])

    result = await db.query(query_params._replace(max_results=None))
    assert result.more_follows is True
    if order == common.Order.DESCENDING:
        assert list(result.events) == [events[3], events[2], events[1]]
    elif order == common.Order.ASCENDING:
        assert list(result.events) == [events[0], events[1], events[2]]

    await write_timeseries_db(env, db)

    result = await db.query(query_params._replace(max_results=None))
    assert result.more_follows is True
    if order == common.Order.DESCENDING:
        assert list(result.events) == [events[3], events[2], events[1]]
    elif order == common.Order.ASCENDING:
        assert list(result.events) == [events[0], events[1], events[2]]

    await env.async_close()


@pytest.mark.parametrize('order', common.Order)
async def test_query_timestamps(order, db_path):
    events = []
    for i in range(4):
        await asyncio.sleep(0.001)
        events.append(common.Event(id=common.EventId(1, 1, i + 1),
                                   type=('a', ),
                                   timestamp=common.now(),
                                   source_timestamp=common.now(),
                                   payload=None))

    env = await environment.create(db_path)
    db = await create_timeseries_db(env, max_results=len(events))

    for event in events:
        add_timeseries_db(db, event)

    query_params = common.QueryTimeseriesParams(order=order)

    for _ in range(2):
        result = await db.query(query_params)
        if order == common.Order.DESCENDING:
            assert list(result.events) == [events[3], events[2], events[1],
                                           events[0]]
        elif order == common.Order.ASCENDING:
            assert list(result.events) == [events[0], events[1], events[2],
                                           events[3]]

        result = await db.query(
            query_params._replace(t_from=events[1].timestamp))
        if order == common.Order.DESCENDING:
            assert list(result.events) == [events[3], events[2], events[1]]
        elif order == common.Order.ASCENDING:
            assert list(result.events) == [events[1], events[2], events[3]]

        result = await db.query(
            query_params._replace(source_t_from=events[1].source_timestamp))
        if order == common.Order.DESCENDING:
            assert list(result.events) == [events[3], events[2], events[1]]
        elif order == common.Order.ASCENDING:
            assert list(result.events) == [events[1], events[2], events[3]]

        result = await db.query(
            query_params._replace(t_to=events[2].timestamp))
        if order == common.Order.DESCENDING:
            assert list(result.events) == [events[2], events[1], events[0]]
        elif order == common.Order.ASCENDING:
            assert list(result.events) == [events[0], events[1], events[2]]

        result = await db.query(
            query_params._replace(source_t_to=events[2].source_timestamp))
        if order == common.Order.DESCENDING:
            assert list(result.events) == [events[2], events[1], events[0]]
        elif order == common.Order.ASCENDING:
            assert list(result.events) == [events[0], events[1], events[2]]

        result = await db.query(
            query_params._replace(t_from=events[0].timestamp,
                                  t_to=events[3].timestamp,
                                  source_t_from=events[0].source_timestamp,
                                  source_t_to=events[3].source_timestamp))
        if order == common.Order.DESCENDING:
            assert list(result.events) == [events[3], events[2], events[1],
                                           events[0]]
        elif order == common.Order.ASCENDING:
            assert list(result.events) == [events[0], events[1], events[2],
                                           events[3]]

        t = common.now()
        result = await db.query(
            query_params._replace(t_from=t, t_to=t, source_t_from=t,
                                  source_t_to=t))
        assert list(result.events) == []

        await write_timeseries_db(env, db)

    await env.async_close()


async def test_query_event_types(db_path):
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=common.now(),
                           payload=None)
              for i in range(4)]

    env = await environment.create(db_path)
    db = await create_timeseries_db(env, max_results=len(events))

    for event in events:
        add_timeseries_db(db, event)

    for _ in range(2):
        for event in events:
            result = await db.query(
                common.QueryTimeseriesParams(event_types=[event.type]))
            assert list(result.events) == [event]

        await write_timeseries_db(env, db)

    await env.async_close()


async def test_query_last_event_id(db_path):
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=common.now(),
                           payload=None)
              for i in range(4)]

    env = await environment.create(db_path)
    db = await create_timeseries_db(env, max_results=len(events))

    for event in events:
        add_timeseries_db(db, event)

    for _ in range(2):
        params = common.QueryTimeseriesParams(max_results=2)
        result = await db.query(params)

        assert result.more_follows
        assert list(result.events) == [events[3], events[2]]

        params = common.QueryTimeseriesParams(last_event_id=events[2].id)
        result = await db.query(params)

        assert not result.more_follows
        assert list(result.events) == [events[1], events[0]]

        await write_timeseries_db(env, db)

    await env.async_close()


async def test_limit_max_entries(db_path):
    limit = Limit(max_entries=2)
    partition = Partition(order_by=common.OrderBy.TIMESTAMP,
                          subscription=common.create_subscription([('*', )]),
                          limit=limit)
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(4)]

    env = await environment.create(db_path)
    db = await create_timeseries_db(env,
                                    partitions=[partition],
                                    max_results=len(events))

    for event in events:
        add_timeseries_db(db, event)

    await write_timeseries_db(env, db)

    params = common.QueryTimeseriesParams()
    result = await db.query(params)

    assert not result.more_follows
    assert list(result.events) == [events[3], events[2], events[1], events[0]]

    await cleanup_timeseries_db(env, db)

    params = common.QueryTimeseriesParams()
    result = await db.query(params)

    assert not result.more_follows
    assert list(result.events) == [events[3], events[2]]

    await env.async_close()


async def test_limit_duration(db_path):
    limit = Limit(min_entries=2,
                  duration=0)
    partition = Partition(order_by=common.OrderBy.TIMESTAMP,
                          subscription=common.create_subscription([('*', )]),
                          limit=limit)
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(4)]

    env = await environment.create(db_path)
    db = await create_timeseries_db(env,
                                    partitions=[partition],
                                    max_results=len(events))

    for event in events:
        add_timeseries_db(db, event)

    await write_timeseries_db(env, db)

    params = common.QueryTimeseriesParams()
    result = await db.query(params)

    assert not result.more_follows
    assert list(result.events) == [events[3], events[2], events[1], events[0]]

    await asyncio.sleep(0.001)

    await cleanup_timeseries_db(env, db)

    params = common.QueryTimeseriesParams()
    result = await db.query(params)

    assert not result.more_follows
    assert list(result.events) == [events[3], events[2]]

    await env.async_close()


async def test_limit_size(db_path):
    limit = Limit(min_entries=2,
                  size=0)
    partition = Partition(order_by=common.OrderBy.TIMESTAMP,
                          subscription=common.create_subscription([('*', )]),
                          limit=limit)
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(4)]

    env = await environment.create(db_path)
    db = await create_timeseries_db(env,
                                    partitions=[partition],
                                    max_results=len(events))

    for event in events:
        add_timeseries_db(db, event)

    await write_timeseries_db(env, db)

    params = common.QueryTimeseriesParams()
    result = await db.query(params)

    assert not result.more_follows
    assert list(result.events) == [events[3], events[2], events[1], events[0]]

    await cleanup_timeseries_db(env, db)

    params = common.QueryTimeseriesParams()
    result = await db.query(params)

    assert not result.more_follows
    assert list(result.events) == [events[3], events[2]]

    await env.async_close()
