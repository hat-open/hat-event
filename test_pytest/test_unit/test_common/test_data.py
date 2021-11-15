import pytest

from hat.event.server import common


@pytest.mark.parametrize("event", [
    common.Event(
        event_id=common.EventId(0, 0),
        event_type=('a',),
        timestamp=common.now(),
        source_timestamp=None,
        payload=None),

    common.Event(
        event_id=common.EventId(0, 0),
        event_type=('a',),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayload(
            type=common.EventPayloadType.BINARY,
            data=b'123')),

    common.Event(
        event_id=common.EventId(123, 456),
        event_type=('a', 'b', 'c'),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayload(
            type=common.EventPayloadType.JSON,
            data=None)),

    common.Event(
        event_id=common.EventId(123, 456),
        event_type=('a', 'b', 'c'),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayload(
            type=common.EventPayloadType.JSON,
            data=[{}, None, [], True, False, 1, 2.5, "123"]))
])
def test_event_sbs(event):
    encoded = common.event_to_sbs(event)
    decoded = common.event_from_sbs(encoded)
    assert decoded == event


@pytest.mark.parametrize("register_event", [
    common.RegisterEvent(
        event_type=('a',),
        source_timestamp=None,
        payload=None),

    common.RegisterEvent(
        event_type=('a',),
        source_timestamp=common.now(),
        payload=common.EventPayload(
            type=common.EventPayloadType.SBS,
            data=common.SbsData(None, 'Integer', b'123'))),

    common.RegisterEvent(
        event_type=('a',),
        source_timestamp=common.now(),
        payload=common.EventPayload(
            type=common.EventPayloadType.BINARY,
            data=b'123')),

    common.RegisterEvent(
        event_type=('a', 'b', 'c'),
        source_timestamp=common.now(),
        payload=common.EventPayload(
            type=common.EventPayloadType.JSON,
            data=None)),

    common.RegisterEvent(
        event_type=('a', 'b', 'c'),
        source_timestamp=common.now(),
        payload=common.EventPayload(
            type=common.EventPayloadType.JSON,
            data=[{}, None, [], True, False, 1, 2.5, "123"]))
])
def test_register_event_sbs(register_event):
    encoded = common.register_event_to_sbs(register_event)
    decoded = common.register_event_from_sbs(encoded)
    assert decoded == register_event


@pytest.mark.parametrize("query", [
    common.QueryData()
])
def test_query_sbs(query):
    encoded = common.query_to_sbs(query)
    decoded = common.query_from_sbs(encoded)
    assert decoded == query
