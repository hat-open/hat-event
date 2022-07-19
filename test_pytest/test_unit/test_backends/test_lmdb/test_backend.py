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
    instance_cntrs = {}

    def create_event(event_type, server_id=1, session=1):
        nonlocal instance_cntrs
        if session not in instance_cntrs:
            instance_cntrs[session] = itertools.count(1)
        instance = next(instance_cntrs[session])
        event_id = common.EventId(server_id, session, instance)
        t = common.now()
        event = common.Event(event_id=event_id,
                             event_type=event_type,
                             timestamp=t,
                             source_timestamp=t,
                             payload=None)
        return event

    return create_event


async def test_create(db_path):
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

    event = create_event(('a',), server_id=server_id, session=1)
    await backend.register([event])

    event_id = await backend.get_last_event_id(server_id)
    assert event_id == event.event_id

    event_id = await backend.get_last_event_id(server_id + 1)
    assert event_id == common.EventId(server_id + 1, 0, 0)

    event2 = create_event(('a',), server_id=server_id + 1, session=1)
    await backend.register([event2])

    event_id = await backend.get_last_event_id(server_id)
    assert event_id == event.event_id

    event_id = await backend.get_last_event_id(server_id + 1)
    assert event_id == event2.event_id

    events = [create_event(('a',), server_id=server_id, session=2)]
    await backend.register(events)
    event_id = await backend.get_last_event_id(server_id)
    assert event_id == events[-1].event_id

    await backend.async_close()

    backend = await create(conf)

    event_id = await backend.get_last_event_id(server_id)
    assert event_id == events[-1].event_id

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


async def test_register_flushed_cb(db_path, create_event):
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

    events1 = [create_event(('a', str(i)), server_id, session=1)
               for i in range(10)]

    await backend.register(events1)

    assert flushed_events_queue.empty()

    flushed_events = await flushed_events_queue.get()
    assert flushed_events == events1

    events2 = [create_event(('a', str(i)), server_id, session=2)
               for i in range(10)]
    events3 = [create_event(('a', str(i)), server_id, session=3 + i)
               for i in range(10)]
    await backend.register(events2 + events3)

    assert flushed_events_queue.empty()
    flushed_events = await flushed_events_queue.get()
    assert flushed_events == events2
    for i in range(10):
        flushed_events = await flushed_events_queue.get()
        assert flushed_events == [events3[i]]

    assert flushed_events_queue.empty()

    await backend.async_close()


async def test_query_flushed(db_path, create_event):
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

    events1 = [create_event(('a', str(i)), server_id, session=1)
               for i in range(10)]
    await backend.register(events1)

    assert flushed_events_queue.empty()

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 0, 0))]
    assert not query_flushed_events

    await flushed_events_queue.get()

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 0, 0))]
    assert query_flushed_events == [events1]

    events2 = [create_event(('a', str(i)), server_id, session=2)
               for i in range(10)]
    await backend.register(events2)

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 0, 0))]
    assert query_flushed_events == [events1]

    await flushed_events_queue.get()

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 0, 0))]
    assert query_flushed_events == [events1, events2]

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 1, 5))]
    assert query_flushed_events == [events1[5:], events2]

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 2, 3))]
    assert query_flushed_events == [events2[3:]]

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 2, 10))]
    assert query_flushed_events == []

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 3, 0))]
    assert query_flushed_events == []

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(2, 3, 0))]
    assert query_flushed_events == []

    events3 = [create_event(('a', str(i)), server_id, session=2)
               for i in range(10)]
    events4 = [create_event(('a', str(i)), server_id, session=3)
               for i in range(10)]
    await backend.register(events3 + events4)
    await flushed_events_queue.get()

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 0, 0))]
    assert query_flushed_events == [events1, events2 + events3, events4]

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 2, 7))]
    assert query_flushed_events == [events2[7:] + events3, events4]

    await backend.async_close()
    backend = await create(conf)

    query_flushed_events = [i async for i in backend.query_flushed(
                            common.EventId(server_id, 0, 0))]
    assert query_flushed_events == [events1, events2 + events3, events4]

    await backend.async_close()
