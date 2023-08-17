import pytest

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import environment
import hat.event.server.backends.lmdb.systemdb


async def create_system_db(env, version, identifier=None):

    def ext_create():
        with env.ext_begin(write=True) as txn:
            return hat.event.server.backends.lmdb.systemdb.ext_create(
                env=env,
                txn=txn,
                version=version,
                identifier=identifier)

    return await env.execute(ext_create)


async def write_system_db(env, db):

    def ext_write(changes):
        with env.ext_begin(write=True) as txn:
            db.ext_write(txn, changes)

    await env.execute(ext_write, db.create_changes())


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'db'


async def test_create(db_path):
    env = await environment.create(db_path)

    db = await create_system_db(env, 123)
    assert db

    await env.async_close()


async def test_create_version(db_path):
    env = await environment.create(db_path)

    db = await create_system_db(env, 123)
    assert db

    with pytest.raises(Exception):
        await create_system_db(env, 321)

    db = await create_system_db(env, 123)
    assert db

    await env.async_close()


async def test_create_identifier(db_path):
    env = await environment.create(db_path)

    db = await create_system_db(env, 123, 'abc')
    assert db

    with pytest.raises(Exception):
        await create_system_db(env, 123, 'xyz')

    db = await create_system_db(env, 123)
    assert db

    db = await create_system_db(env, 123, 'abc')
    assert db

    await env.async_close()


async def test_last_event_id(db_path):
    server_id = 123
    event_id = common.EventId(server_id, 2, 3)

    env = await environment.create(db_path)
    db = await create_system_db(env, 123)

    result = db.get_last_event_id(server_id)
    assert result == common.EventId(server_id, 0, 0)

    db.set_last_event_id(event_id)
    result = db.get_last_event_id(server_id)
    assert result == event_id

    await write_system_db(env, db)
    await env.async_close()

    env = await environment.create(db_path)
    db = await create_system_db(env, 123)

    result = db.get_last_event_id(server_id)
    assert result == event_id

    result = db.get_last_event_id(server_id + 1)
    assert result == common.EventId(server_id + 1, 0, 0)

    await env.async_close()


async def test_last_timestamp(db_path):
    server_id = 123
    timestamp = common.now()

    env = await environment.create(db_path)
    db = await create_system_db(env, 123)

    result = db.get_last_timestamp(server_id)
    assert result == common.min_timestamp

    db.set_last_timestamp(server_id, timestamp)
    result = db.get_last_timestamp(server_id)
    assert result == timestamp

    await write_system_db(env, db)
    await env.async_close()

    env = await environment.create(db_path)
    db = await create_system_db(env, 123)

    result = db.get_last_timestamp(server_id)
    assert result == timestamp

    result = db.get_last_timestamp(server_id + 1)
    assert result == common.min_timestamp

    await env.async_close()
