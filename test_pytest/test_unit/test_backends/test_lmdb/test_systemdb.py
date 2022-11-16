import pytest

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import environment
import hat.event.server.backends.lmdb.systemdb


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


async def create_system_db(env):
    return await env.execute(
        hat.event.server.backends.lmdb.systemdb.ext_create, env)


async def flush(env, db):
    with env.ext_begin(write=True) as txn:
        await env.execute(db.create_ext_flush(), txn, common.now())


async def test_create(env):
    db = await create_system_db(env)

    for server_id in [1, 3, 77]:
        last_event_id, timestamp = db.get_last_event_id_timestamp(server_id)
        assert last_event_id == common.EventId(
            server=server_id, session=0, instance=0)
        assert timestamp == common.Timestamp(-(1 << 63), 0)

    await flush(env, db)


async def test_change(env):
    server1 = 123
    server2 = 456
    db = await create_system_db(env)

    await flush(env, db)

    last_event_id, timestamp = db.get_last_event_id_timestamp(server1)
    assert last_event_id == common.EventId(
        server=server1, session=0, instance=0)
    assert timestamp == common.Timestamp(-(1 << 63), 0)

    timestamp = common.now()
    event_id = common.EventId(server=server1, session=456, instance=0xFFFF)
    db.set_last_event_id_timestamp(event_id, timestamp)

    last_event_id, last_timestamp = db.get_last_event_id_timestamp(server1)
    assert last_event_id == event_id
    assert last_timestamp == timestamp

    last_event_id, last_timestamp = db.get_last_event_id_timestamp(server2)
    assert last_event_id == common.EventId(
        server=server2, session=0, instance=0)
    assert last_timestamp == common.Timestamp(-(1 << 63), 0)

    timestamp1 = common.now()
    event_id1 = common.EventId(server=server1, session=234, instance=789)
    db.set_last_event_id_timestamp(event_id1, timestamp1)
    last_event_id, last_timestamp = db.get_last_event_id_timestamp(server1)
    assert last_event_id == event_id1
    assert last_timestamp == timestamp1

    timestamp2 = common.now()
    event_id2 = common.EventId(server=server2, session=445, instance=112233)
    db.set_last_event_id_timestamp(event_id2, timestamp2)
    last_event_id, last_timestamp = db.get_last_event_id_timestamp(server2)
    assert last_event_id == event_id2
    assert last_timestamp == timestamp2

    await flush(env, db)

    db = await create_system_db(env)

    last_event_id, timestamp = db.get_last_event_id_timestamp(server1)
    assert last_event_id == event_id1
    assert timestamp == timestamp1

    last_event_id, timestamp = db.get_last_event_id_timestamp(server2)
    assert last_event_id == event_id2
    assert timestamp == timestamp2
