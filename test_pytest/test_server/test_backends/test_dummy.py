import pytest

from hat import aio

from hat.event.server import common
from hat.event.server.backends.dummy import info


async def test_create():
    backend = await aio.call(info.create, {}, None, None)

    assert backend.is_open

    await backend.async_close()


@pytest.mark.parametrize("server_id", [0, 1, 5, 10])
async def test_get_last_event_id(server_id):
    backend = await aio.call(info.create, {}, None, None)

    event_id = await backend.get_last_event_id(server_id)
    assert event_id == common.EventId(server_id, 0, 0)

    await backend.async_close()


@pytest.mark.parametrize("event_count", [0, 1, 5, 10])
async def test_register(event_count):
    events = [
        common.Event(
            id=common.EventId(server=1, session=1, instance=i),
            type=(),
            timestamp=common.now(),
            source_timestamp=None,
            payload=None)
        for i in range(event_count)]

    registered_queue = aio.Queue()
    flushed_queue = aio.Queue()
    backend = await aio.call(info.create, {},
                             registered_queue.put_nowait,
                             flushed_queue.put_nowait)

    result = await backend.register(events)
    assert result == events

    result = await registered_queue.get()
    assert result == events

    result = await flushed_queue.get()
    assert result == events

    assert registered_queue.empty()
    assert flushed_queue.empty()

    await backend.async_close()


@pytest.mark.parametrize("params", [common.QueryLatestParams(),
                                    common.QueryTimeseriesParams(),
                                    common.QueryServerParams(123)])
async def test_query(params):
    backend = await aio.call(info.create, {}, None, None)

    result = await backend.query(params)
    assert result == common.QueryResult(events=[],
                                        more_follows=False)

    await backend.async_close()
