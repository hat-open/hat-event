import asyncio
import collections

import pytest

from hat import aio
from hat import util

from hat.event.server import common
import hat.event.eventer


@pytest.fixture
def port():
    return util.get_unused_tcp_port()


@pytest.fixture
def address(port):
    return f'tcp+sbs://127.0.0.1:{port}'


@pytest.mark.parametrize("client_count", [1, 2, 5])
async def test_client_connect_disconnect(address, client_count):
    with pytest.raises(Exception):
        await hat.event.eventer.connect(address)

    server = await hat.event.eventer.listen(address)

    clients = []
    for _ in range(client_count):
        client = await hat.event.eventer.connect(address)
        clients.append(client)

    assert server.is_open
    for client in clients:
        assert client.is_open

    await server.async_close()
    for client in clients:
        await client.wait_closed()

    with pytest.raises(Exception):
        await hat.event.eventer.connect(address)


@pytest.mark.parametrize("subscriptions", [
    [()],
    [('*',)],
    [('?',)],
    [('?', '*')],
    [('?', '?')],
    [('?', '?', '*')],
    [('?', '?', '?')],
    [('?', '?', '?', '*')],
    [('a',)],
    [('a', '*')],
    [('a', '?')],
    [('a', '?', '*')],
    [('a', '?', '?')],
    [('a', 'b')],
    [('a', 'b', '*')],
    [('a', 'b', '?')],
    [('?', 'b')],
    [('?', 'b', '?')],
    [('?', 'b', '*')],
    [('a', 'b', 'c')],
    [('a',), ()],
    [('a',), ('*',)],
    [('a',), ('?',)],
    [('a',), ('b',)],
    [('a',), ('a', 'a')],
    [('a',), ('a', 'b')],
    [('b',), ('a', 'b')],
    [('a',), ('a', 'b'), ('a', 'b', 'c')],
    [('', '', '')],
    [('x', 'y', 'z')]
])
async def test_subscribe(address, subscriptions):
    subscription = common.Subscription(subscriptions)
    event_types = [(),
                   ('a',),
                   ('b',),
                   ('a', 'a'),
                   ('a', 'b'),
                   ('a', 'b', 'c'),
                   ('', '', '')]
    filtered_event_types = [event_type for event_type in event_types
                            if subscription.matches(event_type)]
    events = [
        hat.event.common.Event(
            event_id=hat.event.common.EventId(server=0, session=1, instance=i),
            event_type=event_type,
            timestamp=common.now(),
            source_timestamp=None,
            payload=None)
        for i, event_type in enumerate(event_types)]

    server = await hat.event.eventer.listen(address)
    client = await hat.event.eventer.connect(address, subscriptions)
    await client.query(common.QueryData())  # process `Subscribe` message

    server.notify(events)
    if filtered_event_types:
        events = await client.receive()
        assert ({tuple(i.event_type) for i in events} ==
                {tuple(i) for i in filtered_event_types})

    await client.async_close()
    await server.async_close()


async def test_without_subscribe(address):
    event_types = [(),
                   ('a',),
                   ('b',),
                   ('a', 'a'),
                   ('a', 'b'),
                   ('a', 'b', 'c'),
                   ('', '', '')]
    events = [
        hat.event.common.Event(
            event_id=hat.event.common.EventId(server=0, session=1, instance=i),
            event_type=event_type,
            timestamp=common.now(),
            source_timestamp=None,
            payload=None)
        for i, event_type in enumerate(event_types)]

    server = await hat.event.eventer.listen(address)
    client = await hat.event.eventer.connect(address)
    await client.query(common.QueryData())

    server.notify(events)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(client.receive(), 0.01)

    await client.async_close()
    await server.async_close()


@pytest.mark.parametrize("client_count", [1, 2, 5])
async def test_connected_disconnected_cb(address, client_count):
    connected_queue = aio.Queue()
    disconnected_queue = aio.Queue()

    server = await hat.event.eventer.listen(
        address,
        connected_cb=connected_queue.put_nowait,
        disconnected_cb=disconnected_queue.put_nowait)

    clients = collections.deque()
    for _ in range(client_count):
        client = await hat.event.eventer.connect(address)
        client_id = await connected_queue.get()

        assert client_id not in {i for i, _ in clients}

        clients.append((client_id, client))

    while clients:
        client_id, client = clients.popleft()
        await client.async_close()

        cid = await disconnected_queue.get()

        assert cid == client_id

    await server.async_close()


async def test_register(address):
    register_events = [
        hat.event.common.RegisterEvent(
            event_type=('test', 'a'),
            source_timestamp=None,
            payload=None),
        hat.event.common.RegisterEvent(
            event_type=('test', 'a'),
            source_timestamp=hat.event.common.Timestamp(s=0, us=0),
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data={'a': True, 'b': [0, 1, None, 'c']})),
        hat.event.common.RegisterEvent(
            event_type=('test', 'b'),
            source_timestamp=None,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.BINARY,
                data=b'Test')),
        hat.event.common.RegisterEvent(
            event_type=('test',),
            source_timestamp=None,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.SBS,
                data=hat.event.common.SbsData(module=None, type='Bytes',
                                              data=b'Test')))]

    register_queue = aio.Queue()

    def register_cb(client_id, events):
        register_queue.put_nowait(events)
        return [None for event in events]

    server = await hat.event.eventer.listen(address,
                                            register_cb=register_cb)
    client = await hat.event.eventer.connect(address)

    client.register(register_events)

    events = await register_queue.get()
    assert events == register_events

    await client.async_close()
    await server.async_close()


async def test_register_with_response(address):
    event_types = [(),
                   ('a',),
                   ('b',),
                   ('a', 'a'),
                   ('a', 'b'),
                   ('a', 'b', 'c'),
                   ('', '', '')]
    register_events = [
        hat.event.common.RegisterEvent(
            event_type=event_type,
            source_timestamp=None,
            payload=None)
        for event_type in event_types]

    register_queue = aio.Queue()

    def register_cb(client_id, events):
        register_queue.put_nowait(events)
        return [common.Event(event_id=common.EventId(0, 1, 2),
                             event_type=i.event_type,
                             timestamp=common.now(),
                             source_timestamp=i.source_timestamp,
                             payload=i.payload)
                for i in events]

    server = await hat.event.eventer.listen(address,
                                            register_cb=register_cb)
    client = await hat.event.eventer.connect(address)

    events = await client.register_with_response(register_events)

    temp_register_events = await register_queue.get()
    assert temp_register_events == register_events

    assert [i.event_type for i in events] == event_types

    await client.async_close()
    await server.async_close()


async def test_register_with_response_failure(address):
    event_types = [(),
                   ('a',),
                   ('b',),
                   ('a', 'a'),
                   ('a', 'b'),
                   ('a', 'b', 'c'),
                   ('', '', '')]
    register_events = [
        hat.event.common.RegisterEvent(
            event_type=event_type,
            source_timestamp=None,
            payload=None)
        for event_type in event_types]

    register_queue = aio.Queue()

    def register_cb(client_id, events):
        register_queue.put_nowait(events)
        return [None for _ in events]

    server = await hat.event.eventer.listen(address,
                                            register_cb=register_cb)
    client = await hat.event.eventer.connect(address)

    events = await client.register_with_response(register_events)

    temp_register_events = await register_queue.get()
    assert temp_register_events == register_events

    assert events == [None] * len(register_events)

    await client.async_close()
    await server.async_close()


async def test_query(address):
    event_types = [(),
                   ('a',),
                   ('b',),
                   ('a', 'a'),
                   ('a', 'b'),
                   ('a', 'b', 'c'),
                   ('', '', '')]
    events = [
        hat.event.common.Event(
            event_id=hat.event.common.EventId(server=0, session=1, instance=i),
            event_type=event_type,
            timestamp=common.now(),
            source_timestamp=None,
            payload=None)
        for i, event_type in enumerate(event_types)]
    query_data = common.QueryData()

    query_queue = aio.Queue()

    def query_cb(client_id, data):
        query_queue.put_nowait(data)
        return events

    server = await hat.event.eventer.listen(address,
                                            query_cb=query_cb)
    client = await hat.event.eventer.connect(address)

    result = await client.query(query_data)
    assert result == events

    temp_query_data = await query_queue.get()
    assert temp_query_data == query_data

    await client.async_close()
    await server.async_close()
