import asyncio

import pytest

from hat import aio

from hat.event.backends.lmdb import common
from hat.event.backends.lmdb import info


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'db'


@pytest.fixture
def create_conf(db_path):

    def create_conf(identifier=None,
                    flush_period=1,
                    cleanup_period=1,
                    conditions=[],
                    latest_subscriptions=[['*']],
                    timeseries=[{'order_by': 'TIMESTAMP',
                                 'subscriptions': [['*']]},
                                {'order_by': 'SOURCE_TIMESTAMP',
                                 'subscriptions': [['*']]}]):
        return {'db_path': str(db_path),
                'identifier': identifier,
                'flush_period': flush_period,
                'cleanup_period': cleanup_period,
                'conditions': conditions,
                'latest': {'subscriptions': latest_subscriptions},
                'timeseries': timeseries}

    return create_conf


def test_validate_conf(create_conf):
    conf = create_conf()
    info.json_schema_repo.validate(info.json_schema_id, conf)


async def test_create(create_conf):
    conf = create_conf()

    backend = await aio.call(info.create, conf, None, None)

    assert backend.is_open

    await backend.async_close()


async def test_get_last_event_id(create_conf):
    server_id = 1
    event_id = common.EventId(server_id, 123, 321)

    conf = create_conf()

    backend = await aio.call(info.create, conf, None, None)

    last_event_id = await backend.get_last_event_id(server_id)
    assert last_event_id == common.EventId(server_id, 0, 0)

    event = common.Event(id=event_id,
                         type=('a', ),
                         timestamp=common.now(),
                         source_timestamp=None,
                         payload=None)
    await backend.register([event])

    last_event_id = await backend.get_last_event_id(server_id)
    assert last_event_id == event_id

    event = common.Event(id=event_id._replace(instance=event_id.instance + 1),
                         type=('a', ),
                         timestamp=common.now(),
                         source_timestamp=None,
                         payload=None)
    await backend.register([event])

    last_event_id = await backend.get_last_event_id(server_id)
    assert last_event_id == event_id._replace(instance=event_id.instance + 1)

    await backend.async_close()


async def test_query(create_conf):
    server_id = 1
    event_id = common.EventId(server_id, 123, 321)
    event = common.Event(id=event_id,
                         type=('a', ),
                         timestamp=common.now(),
                         source_timestamp=None,
                         payload=None)

    conf = create_conf()

    backend = await aio.call(info.create, conf, None, None)

    await backend.register([event])

    for _ in range(2):
        for params in [common.QueryLatestParams(),
                       common.QueryTimeseriesParams(),
                       common.QueryServerParams(server_id)]:
            result = await backend.query(params)

            assert list(result.events) == [event]
            assert not result.more_follows

        await backend.flush()

    await backend.async_close()


async def test_cleanup(create_conf):
    server_id = 1
    event_id = common.EventId(server_id, 123, 321)
    event = common.Event(id=event_id,
                         type=('a', ),
                         timestamp=common.now(),
                         source_timestamp=None,
                         payload=None)

    conf = create_conf(flush_period=0.001,
                       cleanup_period=0.001,
                       latest_subscriptions=[],
                       timeseries=[{'order_by': 'TIMESTAMP',
                                    'subscriptions': [['*']],
                                    'limit': {'max_entries': 0}}])

    backend = await aio.call(info.create, conf, None, None)

    await backend.register([event])

    await asyncio.sleep(0.1)

    for params in [common.QueryTimeseriesParams(),
                   common.QueryServerParams(server_id)]:
        result = await backend.query(params)

        assert list(result.events) == []
        assert not result.more_follows

    await backend.async_close()
