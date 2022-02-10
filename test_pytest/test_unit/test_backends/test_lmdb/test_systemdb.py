import datetime

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
        now = datetime.datetime.now(datetime.timezone.utc)
        try:
            await executor(db.create_ext_flush(), txn, now)
        finally:
            await executor(txn.commit)

    return flush


async def test_create_empty(executor, env, flush):
    server_id = 123
    db = await hat.event.server.backends.lmdb.systemdb.create(
        executor=executor,
        env=env,
        server_id=server_id)

    assert db.server_id == server_id
    assert db.last_instance_id is None
    assert db.last_timestamp is None

    await flush(db)

    db = await hat.event.server.backends.lmdb.systemdb.create(
        executor=executor,
        env=env,
        server_id=server_id)

    assert db.server_id == server_id
    assert db.last_instance_id is None
    assert db.last_timestamp is None


async def test_change(executor, env, flush):
    server_id = 123
    db = await hat.event.server.backends.lmdb.systemdb.create(
        executor=executor,
        env=env,
        server_id=server_id)

    await flush(db)

    assert db.server_id == server_id
    assert db.last_instance_id is None
    assert db.last_timestamp is None

    t = common.now()
    db.change(123, t)

    assert db.server_id == server_id
    assert db.last_instance_id == 123
    assert db.last_timestamp == t

    await flush(db)

    db = await hat.event.server.backends.lmdb.systemdb.create(
        executor=executor,
        env=env,
        server_id=server_id)

    assert db.server_id == server_id
    assert db.last_instance_id == 123
    assert db.last_timestamp == t


async def test_invalid_server_id(executor, env, flush):
    server_id = 123
    db = await hat.event.server.backends.lmdb.systemdb.create(
        executor=executor,
        env=env,
        server_id=server_id)

    await flush(db)

    with pytest.raises(Exception):
        await hat.event.server.backends.lmdb.systemdb.create(
            executor=executor,
            env=env,
            server_id=server_id + 1)
