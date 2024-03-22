import pytest

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import environment
from hat.event.server.backends.lmdb.conditions import Conditions
import hat.event.server.backends.lmdb.latestdb


async def create_latest_db(env,
                           conditions=Conditions([]),
                           subscription=common.create_subscription([('*', )])):

    def ext_create():
        with env.ext_begin(write=True) as txn:
            return hat.event.server.backends.lmdb.latestdb.ext_create(
                env=env,
                txn=txn,
                conditions=conditions,
                subscription=subscription)

    return await env.execute(ext_create)


async def write_latest_db(env, db):

    def ext_write(changes):
        with env.ext_begin(write=True) as txn:
            db.ext_write(txn, changes)

    await env.execute(ext_write, db.create_changes())


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'db'


async def test_create(db_path):
    env = await environment.create(db_path)

    db = await create_latest_db(env)
    assert db

    await env.async_close()


@pytest.mark.parametrize('events_count', [1, 5])
async def test_add(events_count, db_path):
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(events_count)]

    env = await environment.create(db_path)
    db = await create_latest_db(env)

    result = db.query(common.QueryLatestParams())
    assert result.events == []

    for event in events:
        result = db.add(event)
        assert result.added_ref
        assert not result.removed_ref

    result = db.query(common.QueryLatestParams())
    assert set(result.events) == set(events)

    await write_latest_db(env, db)
    await env.async_close()

    env = await environment.create(db_path)
    db = await create_latest_db(env)

    result = db.query(common.QueryLatestParams())
    assert set(result.events) == set(events)

    await env.async_close()


async def test_add_replace(db_path):
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', 'b', 'c'),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(2)]

    env = await environment.create(db_path)
    db = await create_latest_db(env)

    result = db.add(events[0])
    assert result.added_ref
    assert not result.removed_ref

    result = db.add(events[1])
    assert result.added_ref
    assert result.removed_ref
    assert result.removed_ref[0] == events[0].id

    result = db.query(common.QueryLatestParams())
    assert result.events == [events[1]]

    await write_latest_db(env, db)
    await env.async_close()

    env = await environment.create(db_path)
    db = await create_latest_db(env)

    result = db.query(common.QueryLatestParams())
    assert result.events == [events[1]]

    await env.async_close()


@pytest.mark.parametrize('events_count', [1, 5])
async def test_query(events_count, db_path):
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(events_count)]

    env = await environment.create(db_path)
    db = await create_latest_db(env)

    for event in events:
        db.add(event)

    result = db.query(common.QueryLatestParams())
    assert set(result.events) == set(events)

    for event in events:
        result = db.query(common.QueryLatestParams([event.type]))
        assert result.events == [event]

    await write_latest_db(env, db)
    await env.async_close()

    env = await environment.create(db_path)
    db = await create_latest_db(env)

    result = db.query(common.QueryLatestParams([('a', '?')]))
    assert set(result.events) == set(events)

    await env.async_close()


@pytest.mark.parametrize('events_count', [1, 5])
async def test_subscription(events_count, db_path):
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=None)
              for i in range(events_count)]

    env = await environment.create(db_path)
    db = await create_latest_db(
        env,
        subscription=common.create_subscription([events[0].type]))

    for event in events:
        db.add(event)

    result = db.query(common.QueryLatestParams())
    assert result.events == [events[0]]

    await write_latest_db(env, db)
    await env.async_close()

    env = await environment.create(db_path)
    db = await create_latest_db(env)

    result = db.query(common.QueryLatestParams())
    assert result.events == [events[0]]

    await env.async_close()


@pytest.mark.parametrize('events_count', [1, 5])
async def test_conditions(events_count, db_path):
    events = [common.Event(id=common.EventId(1, 1, i + 1),
                           type=('a', str(i)),
                           timestamp=common.now(),
                           source_timestamp=None,
                           payload=common.EventPayloadJson(i))
              for i in range(events_count)]

    env = await environment.create(db_path)
    db = await create_latest_db(env)

    for event in events:
        db.add(event)

    await write_latest_db(env, db)
    await env.async_close()

    env = await environment.create(db_path)
    db = await create_latest_db(
        env,
        conditions=Conditions([{'subscriptions': [list(events[0].type)],
                                'condition': {'type': 'json',
                                              'data_value': -1}}]))

    result = db.query(common.QueryLatestParams())
    assert set(result.events) == set(events[1:])

    await env.async_close()
