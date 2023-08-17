import pytest

from hat.event import common


@pytest.mark.skip(reason="WIP")
def test_event_ordering():
    pass


@pytest.mark.parametrize("status", common.Status)
def test_status_sbs(status):
    encoded = common.status_to_sbs(status)
    decoded = common.status_from_sbs(encoded)
    assert decoded == status


@pytest.mark.parametrize("event", [
    common.Event(
        id=common.EventId(0, 0, 0),
        type=('a',),
        timestamp=common.now(),
        source_timestamp=None,
        payload=None),

    common.Event(
        id=common.EventId(0, 0, 0),
        type=('a',),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayloadBinary(
            type='abc',
            data=b'123')),

    common.Event(
        id=common.EventId(123, 456, 789),
        type=('a', 'b', 'c'),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayloadJson(
            data=None)),

    common.Event(
        id=common.EventId(123, 456, 789),
        type=('a', 'b', 'c'),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayloadJson(
            data=[{}, None, [], True, False, 1, 2.5, "123"]))
])
def test_event_sbs(event):
    encoded = common.event_to_sbs(event)
    decoded = common.event_from_sbs(encoded)
    assert decoded == event


@pytest.mark.parametrize("register_event", [
    common.RegisterEvent(
        type=('a',),
        source_timestamp=None,
        payload=None),

    common.RegisterEvent(
        type=('a',),
        source_timestamp=common.now(),
        payload=common.EventPayloadBinary(
            type='abc',
            data=b'123')),

    common.RegisterEvent(
        type=('a', 'b', 'c'),
        source_timestamp=common.now(),
        payload=common.EventPayloadJson(
            data=None)),

    common.RegisterEvent(
        type=('a', 'b', 'c'),
        source_timestamp=common.now(),
        payload=common.EventPayloadJson(
            data=[{}, None, [], True, False, 1, 2.5, "123"]))
])
def test_register_event_sbs(register_event):
    encoded = common.register_event_to_sbs(register_event)
    decoded = common.register_event_from_sbs(encoded)
    assert decoded == register_event


@pytest.mark.parametrize("params", [
    common.QueryLatestParams(),

    common.QueryLatestParams(
        event_types=[]),

    common.QueryLatestParams(
        event_types=[('a',), ('a', 'b', 'c')]),

    common.QueryTimeseriesParams(),

    common.QueryTimeseriesParams(
        event_types=[('a',), ('a', 'b', 'c')],
        t_from=common.now(),
        t_to=common.now(),
        source_t_from=common.now(),
        source_t_to=common.now(),
        order=common.Order.ASCENDING,
        order_by=common.OrderBy.SOURCE_TIMESTAMP,
        max_results=123,
        last_event_id=common.EventId(1, 2, 3)),

    common.QueryServerParams(
        server_id=123),

    common.QueryServerParams(
        server_id=321,
        persisted=True,
        max_results=123,
        last_event_id=common.EventId(3, 2, 1))
])
def test_query_params_sbs(params):
    encoded = common.query_params_to_sbs(params)
    decoded = common.query_params_from_sbs(encoded)
    assert decoded == params


@pytest.mark.parametrize("result", [
    common.QueryResult(
        events=[],
        more_follows=False),

    common.QueryResult(
        events=[
            common.Event(
                id=common.EventId(0, 0, 0),
                type=('a',),
                timestamp=common.now(),
                source_timestamp=None,
                payload=None)],
        more_follows=False)
])
def test_query_result_sbs(result):
    encoded = common.query_result_to_sbs(result)
    decoded = common.query_result_from_sbs(encoded)
    assert decoded == result
