import asyncio
import itertools
import pytest

from hat import aio
from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb.backend import create, LmdbBackend


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'db'


@pytest.fixture
def create_event():
    session = 1
    next_session = itertools.count(1)
    next_instance = itertools.count(1)

    def create_event(event_type, server_id=1):
        nonlocal session
        instance = next(next_instance)
        session = next(next_session) if instance % 10 == 1 else session
        event_id = common.EventId(server_id, session, instance)
        t = common.now()
        event = common.Event(event_id=event_id,
                             event_type=event_type,
                             timestamp=t,
                             source_timestamp=t,
                             payload=None)
        return event

    return create_event


async def test_create_empty(db_path):
    conf = {'db_path': str(db_path),
            'max_db_size': 1024 * 1024 * 1024,
            'flush_period': 30,
            'server_id': 1,
            'conditions': [],
            'latest': {'subscriptions': [['*']]},
            'ordered': [{'order_by': 'TIMESTAMP',
                         'subscriptions': [['*']]}]}

    backend = await create(conf)

    assert isinstance(backend, LmdbBackend)
    assert backend.is_open

    backend.close()
    await backend.wait_closing()
    assert not backend.is_open
    assert backend.is_closing
    await backend.wait_closed()
    assert backend.is_closed


async def test_get_last_event_id(db_path, create_event):
    server_id = 1
    conf = {'db_path': str(db_path),
            'max_db_size': 1024 * 1024 * 1024,
            'flush_period': 30,
            'server_id': server_id,
            'conditions': [],
            'latest': {'subscriptions': [['*']]},
            'ordered': [{'order_by': 'TIMESTAMP',
                         'subscriptions': [['*']]}]}

    backend = await create(conf)

    event_id = await backend.get_last_event_id(server_id)
    assert event_id == common.EventId(server_id, 0, 0)

    event_id = await backend.get_last_event_id(server_id + 1)
    assert event_id == common.EventId(server_id + 1, 0, 0)

    event = create_event(('a',))
    await backend.register([event])

    event_id = await backend.get_last_event_id(server_id)
    assert event_id == event.event_id

    event_id = await backend.get_last_event_id(server_id + 1)
    assert event_id == common.EventId(server_id + 1, 0, 0)

    await backend.async_close()

    backend = await create(conf)

    event_id = await backend.get_last_event_id(server_id)
    assert event_id == event.event_id

    await backend.async_close()


async def test_register(db_path, create_event):
    conf = {'db_path': str(db_path),
            'max_db_size': 1024 * 1024 * 1024,
            'flush_period': 30,
            'server_id': 1,
            'conditions': [{'subscriptions': [('f',)],
                            'condition': {'type': 'json'}}],
            'latest': {'subscriptions': [['*']]},
            'ordered': [{'order_by': 'TIMESTAMP',
                         'subscriptions': [['*']]}]}

    backend = await create(conf)

    result = await backend.query(common.QueryData())
    assert result == []

    event1 = create_event(('a',))
    await asyncio.sleep(0.001)
    event2 = create_event(('b',), server_id=2)
    await asyncio.sleep(0.001)
    # not registered: invalid id
    event3 = create_event(('c',))._replace(event_id=event1.event_id)
    await asyncio.sleep(0.001)
    event4 = create_event(('d',))
    await asyncio.sleep(0.001)
    # not registered: invalid timestamp
    event5 = create_event(('e',))._replace(timestamp=event1.timestamp)
    await asyncio.sleep(0.001)
    # not registered: invalid conditions
    event6 = create_event(('f',))
    await asyncio.sleep(0.001)
    event7 = create_event(('g',))
    await asyncio.sleep(0.001)

    events = [event1, event2, event3, event4, event5, event6, event7]
    result = await backend.register(events)
    assert result == events

    exp_registered_events = [event7, event4, event2, event1]
    result = await backend.query(common.QueryData())
    assert result == exp_registered_events

    await backend.async_close()

    backend = await create(conf)

    result = await backend.query(common.QueryData())
    assert result == exp_registered_events

    await backend.async_close()


async def test_query(db_path, create_event):
    conf = {'db_path': str(db_path),
            'max_db_size': 1024 * 1024 * 1024,
            'flush_period': 30,
            'server_id': 1,
            'conditions': [],
            'latest': {'subscriptions': [['*']]},
            'ordered': [{'order_by': 'TIMESTAMP',
                         'subscriptions': [['*']]},
                        {'order_by': 'SOURCE_TIMESTAMP',
                         'subscriptions': [['*']]}]}

    backend = await create(conf)

    event1 = create_event(('a',))
    event2 = create_event(('a',))
    event3 = create_event(('a',))

    events = [event1, event2, event3]
    result = await backend.register(events)
    assert result == events

    result = await backend.query(common.QueryData())
    assert result == [event3, event2, event1]

    result = await backend.query(common.QueryData(
        order_by=common.OrderBy.SOURCE_TIMESTAMP))
    assert result == [event3, event2, event1]

    result = await backend.query(common.QueryData(unique_type=True))
    assert result == [event3]

    result = await backend.query(common.QueryData(t_from=event1.timestamp,
                                                  unique_type=True,
                                                  max_results=1))
    assert result == [event3]

    await backend.async_close()


async def test_query_partitioning(db_path, create_event):
    conf = {'db_path': str(db_path),
            'max_db_size': 1024 * 1024 * 1024,
            'flush_period': 30,
            'server_id': 1,
            'conditions': [],
            'latest': {'subscriptions': [['*']]},
            'ordered': [{'order_by': 'TIMESTAMP',
                         'subscriptions': [['a']]},
                        {'order_by': 'TIMESTAMP',
                         'subscriptions': [['b']]}]}

    backend = await create(conf)

    event1 = create_event(('a',))
    event2 = create_event(('b',))

    events = [event1, event2]
    result = await backend.register(events)
    assert result == events

    result = await backend.query(common.QueryData(event_types=[('a',)]))
    assert result == [event1]

    result = await backend.query(common.QueryData(event_types=[('b',)]))
    assert result == [event2]

    result = await backend.query(common.QueryData(event_types=[('*',)]))
    assert result == [event1]

    await backend.async_close()


async def test_flushed_events(db_path, create_event):
    server_id = 1
    conf = {'db_path': str(db_path),
            'max_db_size': 1024 * 1024 * 1024,
            'flush_period': 0.05,
            'server_id': server_id,
            'conditions': [],
            'latest': {'subscriptions': [['*']]},
            'ordered': [{'order_by': 'TIMESTAMP',
                         'subscriptions': [['*']]}]}
    backend = await create(conf)
    flushed_events_queue = aio.Queue()
    backend.register_flushed_events_cb(
        lambda i: flushed_events_queue.put_nowait(i))

    events = [create_event(('a', str(i)), server_id) for i in range(10)]

    await backend.register(events)

    assert flushed_events_queue.empty()

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 0, 0))]
    assert len(query_flushed_events) == 0

    flushed_events = await flushed_events_queue.get()
    assert flushed_events == events

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 0, 0))]
    assert len(query_flushed_events) == 1
    assert query_flushed_events[0] == events

    await backend.async_close()
