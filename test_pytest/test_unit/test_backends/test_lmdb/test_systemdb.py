import lmdb
import pytest

from hat import aio
from hat.event.server.backends.lmdb import common
import hat.event.server.backends.lmdb.systemdb


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

    async def flush(db):
        txn = await executor(env.begin, write=True)
        ctx = common.FlushContext(txn)
        try:
            await executor(db.create_ext_flush(), ctx)
        finally:
            await executor(txn.commit)

    return flush


async def test_create(executor, env, flush):
    db = await hat.event.server.backends.lmdb.systemdb.create(
        executor=executor,
        env=env)

    for server_id in [1, 3, 77]:
        last_event_id, timestamp = db.get_last_event_id_timestamp(server_id)
        assert last_event_id == common.EventId(
            server=server_id, session=0, instance=0)
        assert timestamp == common.Timestamp(-(1 << 63), 0)

    await flush(db)


async def test_change(executor, env, flush):
    server1 = 123
    server2 = 456
    db = await hat.event.server.backends.lmdb.systemdb.create(
        executor=executor,
        env=env)

    await flush(db)

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

    await flush(db)

    db = await hat.event.server.backends.lmdb.systemdb.create(
        executor=executor,
        env=env)

    last_event_id, timestamp = db.get_last_event_id_timestamp(server1)
    assert last_event_id == event_id1
    assert timestamp == timestamp1

    last_event_id, timestamp = db.get_last_event_id_timestamp(server2)
    assert last_event_id == event_id2
    assert timestamp == timestamp2
