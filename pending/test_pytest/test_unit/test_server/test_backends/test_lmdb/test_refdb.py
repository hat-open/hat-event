import pytest

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder
from hat.event.server.backends.lmdb import environment
import hat.event.server.backends.lmdb.refdb


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
    instance_ids = {}

    def create_event(session_id):
        instance_id = instance_ids.get(session_id, 0) + 1
        instance_ids[session_id] = instance_id
        event_id = common.EventId(1, session_id, instance_id)
        return common.Event(
            event_id=event_id,
            event_type=('a', str(instance_id)),
            timestamp=common.now(),
            source_timestamp=None,
            payload=common.EventPayload(type=common.EventPayloadType.JSON,
                                        data=instance_id))

    return create_event


async def add_latest_data(env, event):

    def ext_add_latest_data():
        with env.ext_begin(write=True) as txn:
            with env.ext_cursor(txn, common.DbType.LATEST_DATA) as cursor:
                key = int(event.event_type[1])
                encoded_key = encoder.encode_latest_data_db_key(key)
                encoded_value = encoder.encode_latest_data_db_value(event)
                cursor.put(encoded_key, encoded_value)
                return common.LatestEventRef(key)

    return await env.execute(ext_add_latest_data)


async def add_ordered_data(env, event):

    def ext_add_ordered_data():
        with env.ext_begin(write=True) as txn:
            with env.ext_cursor(txn, common.DbType.ORDERED_DATA) as cursor:
                key = 1, event.timestamp, event.event_id
                encoded_key = encoder.encode_ordered_data_db_key(key)
                encoded_value = encoder.encode_ordered_data_db_value(event)
                cursor.put(encoded_key, encoded_value)
                return common.OrderedEventRef(key)

    return await env.execute(ext_add_ordered_data)


async def add_event_ref(env, ref_db, event_id, ref):
    with env.ext_begin(write=True) as txn:
        return await env.execute(ref_db.ext_add_event_ref, txn, event_id, ref)


async def remove_event_ref(env, ref_db, event_id, ref):
    with env.ext_begin(write=True) as txn:
        return await env.execute(ref_db.ext_remove_event_ref, txn, event_id,
                                 ref)


async def test_create(env):
    db = hat.event.server.backends.lmdb.refdb.RefDb(env)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == []


async def test_add_remove_event_ref(env, create_event):
    db = hat.event.server.backends.lmdb.refdb.RefDb(env)

    event = create_event(1)
    ref1 = await add_latest_data(env, event)
    ref2 = await add_ordered_data(env, event)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == []

    await add_event_ref(env, db, event.event_id, ref1)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [[event]]

    await add_event_ref(env, db, event.event_id, ref2)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [[event]]

    await remove_event_ref(env, db, event.event_id, ref1)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [[event]]

    await remove_event_ref(env, db, event.event_id, ref2)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == []


async def test_query(env, create_event):
    db = hat.event.server.backends.lmdb.refdb.RefDb(env)

    events1 = [create_event(1) for i in range(10)]
    events2 = [create_event(2) for i in range(10)]
    events3 = [create_event(3) for i in range(10)]

    for event in events1:
        ref = await add_ordered_data(env, event)
        await add_event_ref(env, db, event.event_id, ref)

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

    for event in events2:
        ref = await add_ordered_data(env, event)
        await add_event_ref(env, db, event.event_id, ref)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [events1, events2]

    result = [i async for i in db.query(common.EventId(1, 1, 7))]
    assert result == [events1[7:], events2]

    result = [i async for i in db.query(common.EventId(1, 2, 0))]
    assert result == [events2]

    result = [i async for i in db.query(common.EventId(1, 2, 6))]
    assert result == [events2[6:]]

    for event in events3:
        ref = await add_latest_data(env, event)
        await add_event_ref(env, db, event.event_id, ref)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [events1, events2, events3]

    for event in events3:
        ref = await add_ordered_data(env, event)
        await add_event_ref(env, db, event.event_id, ref)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [events1, events2, events3]

    events4 = [create_event(3) for i in range(10)]

    for event in events4:
        ref = await add_latest_data(env, event)
        await add_event_ref(env, db, event.event_id, ref)

        ref = await add_ordered_data(env, event)
        await add_event_ref(env, db, event.event_id, ref)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [events1, events2, events3 + events4]

    result = [i async for i in db.query(common.EventId(1, 3, 5))]
    assert result == [events3[5:] + events4]

    result = [i async for i in db.query(common.EventId(1, 3, 19))]
    assert len(result) == 1
    assert result == [events4[-1:]]

    result = [i async for i in db.query(common.EventId(1, 3, 20))]
    assert not result

    db = hat.event.server.backends.lmdb.refdb.RefDb(env)

    result = [i async for i in db.query(common.EventId(1, 0, 0))]
    assert result == [events1, events2, events3 + events4]
