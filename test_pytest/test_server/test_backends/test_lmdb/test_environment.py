import lmdb
import pytest

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import environment


server_id = 1
event_id = common.EventId(server_id, 2, 3)
event_type_ref = 123
partition_id = 321
timestamp = common.now()
source_timestamp = None
timeseries_key = partition_id, timestamp, event_id
event_ref = common.LatestEventRef(event_type_ref)
event_type = ('x', 'y', 'z')
event_payload = common.EventPayloadJson(42)
event = common.Event(id=event_id,
                     type=event_type,
                     timestamp=timestamp,
                     source_timestamp=source_timestamp,
                     payload=event_payload)


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'db'


async def test_create(db_path):
    env = await environment.create(db_path)
    assert env.is_open
    await env.async_close()


async def test_begin(db_path):

    def ext_test_begin():
        with env.ext_begin() as txn:
            assert isinstance(txn, lmdb.Transaction)

    env = await environment.create(db_path)
    await env.execute(ext_test_begin)
    await env.async_close()


@pytest.mark.parametrize('db_type', common.DbType)
async def test_cursor(db_type, db_path):

    def ext_test_cursor():
        with env.ext_begin() as txn:
            with env.ext_cursor(txn, db_type) as cursor:
                assert isinstance(cursor, lmdb.Cursor)

    env = await environment.create(db_path)
    await env.execute(ext_test_cursor)
    await env.async_close()


@pytest.mark.parametrize('db_type', common.DbType)
async def test_stat(db_type, db_path):

    def ext_test_stat():
        with env.ext_begin() as txn:
            stat = env.ext_stat(txn, db_type)
            assert 'entries' in stat
            assert 'psize' in stat
            assert 'branch_pages' in stat
            assert 'leaf_pages' in stat
            assert 'overflow_pages' in stat

    env = await environment.create(db_path)
    await env.execute(ext_test_stat)
    await env.async_close()


@pytest.mark.parametrize('db_type, key, value', [
    (common.DbType.SYSTEM_SETTINGS, common.SettingsId.VERSION, 123),
    (common.DbType.SYSTEM_LAST_EVENT_ID, server_id, event_id),
    (common.DbType.SYSTEM_LAST_TIMESTAMP, server_id, timestamp),
    (common.DbType.REF, event_id, {event_ref}),
    (common.DbType.LATEST_DATA, event_type_ref, event),
    (common.DbType.LATEST_TYPE, event_type_ref, event_type),
    (common.DbType.TIMESERIES_DATA, timeseries_key, event),
    (common.DbType.TIMESERIES_PARTITION, partition_id, 123),
    (common.DbType.TIMESERIES_COUNT, partition_id, 321)
])
async def test_read_write(db_type, key, value, db_path):

    def ext_test_read_write():
        data = {key: value}

        with env.ext_begin(write=True) as txn:
            result = dict(env.ext_read(txn, db_type))
            assert result == {}

            env.ext_write(txn, db_type, data.items())

            result = dict(env.ext_read(txn, db_type))
            assert result == data

    env = await environment.create(db_path)
    await env.execute(ext_test_read_write)
    await env.async_close()
