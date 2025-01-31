import collections

import pytest

from hat.event.backends.lmdb import common
from hat.event.backends.lmdb import environment
from hat.event.backends.lmdb.conditions import Conditions
from hat.event.backends.lmdb.timeseriesdb import Limit, Partition
import hat.event.backends.lmdb.latestdb
import hat.event.backends.lmdb.refdb
import hat.event.backends.lmdb.timeseriesdb


default_partitions = [
    Partition(order_by=common.OrderBy.TIMESTAMP,
              subscription=common.create_subscription([('*', )]),
              limit=None),
    Partition(order_by=common.OrderBy.SOURCE_TIMESTAMP,
              subscription=common.create_subscription([('*', )]),
              limit=None)]


async def create_latest_db(env,
                           conditions=Conditions([]),
                           subscription=common.create_subscription([('*', )])):

    def ext_create():
        with env.ext_begin(write=True) as txn:
            return hat.event.backends.lmdb.latestdb.ext_create(
                env=env,
                txn=txn,
                conditions=conditions,
                subscription=subscription)

    return await env.execute(ext_create)


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


def add_event(latest_db, timeseries_db, ref_db, event):
    refs = collections.deque()

    latest_result = latest_db.add(event)

    if latest_result.added_ref:
        refs.append(latest_result.added_ref)

    if latest_result.removed_ref:
        ref_db.remove(*latest_result.removed_ref)

    refs.extend(timeseries_db.add(event))

    if refs:
        ref_db.add(event, refs)


async def write_dbs(env, latest_db, timeseries_db, ref_db):

    def ext_write(latest_changes, timeseries_changes, ref_changes):
        with env.ext_begin(write=True) as txn:
            latest_db.ext_write(txn, latest_changes)
            timeseries_db.ext_write(txn, timeseries_changes)
            ref_db.ext_write(txn, ref_changes)

    await env.execute(ext_write, latest_db.create_changes(),
                      timeseries_db.create_changes(), ref_db.create_changes())


async def cleanup_dbs(env, timeseries_db, ref_db, now=None, max_results=None):

    def ext_cleanup(now):
        with env.ext_begin(write=True) as txn:
            result = timeseries_db.ext_cleanup(txn, now, max_results)
            ref_db.ext_cleanup(txn, result)

    return await env.execute(ext_cleanup, now or common.now())


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'db'


async def test_create(db_path):
    env = await environment.create(db_path)

    db = hat.event.backends.lmdb.refdb.RefDb(env)
    assert db

    await env.async_close()


async def test_add(db_path):
    event = common.Event(id=common.EventId(1, 1, 1),
                         type=('a', ),
                         timestamp=common.now(),
                         source_timestamp=None,
                         payload=None)
    refs = [common.LatestEventRef(123),
            common.TimeseriesEventRef((1, event.timestamp, event.id))]

    env = await environment.create(db_path)
    db = hat.event.backends.lmdb.refdb.RefDb(env)

    changes = db.create_changes()
    assert changes == {}

    db.add(event, refs)

    changes = db.create_changes()
    server_changes = changes[event.id.server]
    assert server_changes.added == {event.id: (event, set(refs))}
    assert server_changes.removed == {}

    await env.async_close()


async def test_remove(db_path):
    event = common.Event(id=common.EventId(1, 1, 1),
                         type=('a', ),
                         timestamp=common.now(),
                         source_timestamp=None,
                         payload=None)
    ref = common.LatestEventRef(123)

    env = await environment.create(db_path)
    db = hat.event.backends.lmdb.refdb.RefDb(env)

    changes = db.create_changes()
    assert changes == {}

    db.remove(event.id, ref)

    changes = db.create_changes()
    server_changes = changes[event.id.server]
    assert server_changes.added == {}
    assert server_changes.removed == {event.id: {ref}}

    await env.async_close()


async def test_remove_added(db_path):
    event = common.Event(id=common.EventId(1, 1, 1),
                         type=('a', ),
                         timestamp=common.now(),
                         source_timestamp=None,
                         payload=None)
    refs = [common.LatestEventRef(123),
            common.TimeseriesEventRef((1, event.timestamp, event.id))]

    env = await environment.create(db_path)
    db = hat.event.backends.lmdb.refdb.RefDb(env)

    changes = db.create_changes()
    assert changes == {}

    db.add(event, refs)
    db.remove(event.id, refs[0])

    changes = db.create_changes()
    server_changes = changes[event.id.server]
    assert server_changes.added == {event.id: (event, {refs[1]})}
    assert server_changes.removed == {}

    await env.async_close()


@pytest.mark.parametrize('event_count', [0, 1, 5])
async def test_query(event_count, db_path):
    server_id = 1
    events = [common.Event(id=common.EventId(server_id, 1, i + 1),
                           type=('a', ),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(event_count)]

    env = await environment.create(db_path)
    latest_db = await create_latest_db(env)
    timeseries_db = await create_timeseries_db(env)
    ref_db = hat.event.backends.lmdb.refdb.RefDb(env)

    for event in events:
        add_event(latest_db, timeseries_db, ref_db, event)

    for _ in range(2):
        params = common.QueryServerParams(server_id=server_id)
        result = await ref_db.query(params)

        assert list(result.events) == events
        assert not result.more_follows

        await write_dbs(env, latest_db, timeseries_db, ref_db)

    await env.async_close()


async def test_query_persisted(db_path):
    server_id = 1
    events = [common.Event(id=common.EventId(server_id, 1, i + 1),
                           type=('a', ),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(4)]

    env = await environment.create(db_path)
    latest_db = await create_latest_db(env)
    timeseries_db = await create_timeseries_db(env)
    ref_db = hat.event.backends.lmdb.refdb.RefDb(env)

    for event in events:
        add_event(latest_db, timeseries_db, ref_db, event)

    params = common.QueryServerParams(server_id=server_id,
                                      persisted=True)
    result = await ref_db.query(params)

    assert list(result.events) == []
    assert not result.more_follows

    await write_dbs(env, latest_db, timeseries_db, ref_db)

    params = common.QueryServerParams(server_id=server_id,
                                      persisted=True)
    result = await ref_db.query(params)

    assert list(result.events) == events
    assert not result.more_follows

    await env.async_close()


async def test_query_max_results(db_path):
    server_id = 1
    events = [common.Event(id=common.EventId(server_id, 1, i + 1),
                           type=('a', ),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(4)]

    env = await environment.create(db_path)
    latest_db = await create_latest_db(env)
    timeseries_db = await create_timeseries_db(env)
    ref_db = hat.event.backends.lmdb.refdb.RefDb(env)

    for event in events:
        add_event(latest_db, timeseries_db, ref_db, event)

    for _ in range(2):
        params = common.QueryServerParams(server_id=server_id,
                                          max_results=2)
        result = await ref_db.query(params)

        assert list(result.events) == events[:2]
        assert result.more_follows

        await write_dbs(env, latest_db, timeseries_db, ref_db)

    await env.async_close()


async def test_query_last_event_id(db_path):
    server_id = 1
    events = [common.Event(id=common.EventId(server_id, 1, i + 1),
                           type=('a', ),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(4)]

    env = await environment.create(db_path)
    latest_db = await create_latest_db(env)
    timeseries_db = await create_timeseries_db(env)
    ref_db = hat.event.backends.lmdb.refdb.RefDb(env)

    for event in events:
        add_event(latest_db, timeseries_db, ref_db, event)

    for _ in range(2):
        params = common.QueryServerParams(server_id=server_id,
                                          last_event_id=events[1].id)
        result = await ref_db.query(params)

        assert list(result.events) == events[2:]
        assert not result.more_follows

        await write_dbs(env, latest_db, timeseries_db, ref_db)

    await env.async_close()


async def test_cleanup(db_path):
    server_id = 1
    limit = Limit(max_entries=2)
    partition = Partition(order_by=common.OrderBy.TIMESTAMP,
                          subscription=common.create_subscription([('*', )]),
                          limit=limit)
    events = [common.Event(id=common.EventId(server_id, 1, i + 1),
                           type=('a', ),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(4)]

    env = await environment.create(db_path)
    latest_db = await create_latest_db(env)
    timeseries_db = await create_timeseries_db(env, partitions=[partition])
    ref_db = hat.event.backends.lmdb.refdb.RefDb(env)

    for event in events:
        add_event(latest_db, timeseries_db, ref_db, event)

    await write_dbs(env, latest_db, timeseries_db, ref_db)
    await cleanup_dbs(env, timeseries_db, ref_db)

    params = common.QueryServerParams(server_id=server_id)
    result = await ref_db.query(params)

    assert list(result.events) == events[2:]
    assert not result.more_follows

    await env.async_close()
