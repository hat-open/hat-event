import pytest

from hat.event.mariner import common
from hat.event.mariner import encoder


events = [
    common.Event(
        event_id=common.EventId(1, 1, 1),
        event_type=('a', 'b', 'c'),
        timestamp=common.now(),
        source_timestamp=None,
        payload=None),
    common.Event(
        event_id=common.EventId(1, 2, 3),
        event_type=('a', ),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayload(common.EventPayloadType.JSON,
                                    {'a': [1, True]})),
    common.Event(
        event_id=common.EventId(1, 2, 3),
        event_type=('b', ),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayload(common.EventPayloadType.BINARY,
                                    b'abc')),
    common.Event(
        event_id=common.EventId(1, 2, 3),
        event_type=('c', ),
        timestamp=common.now(),
        source_timestamp=common.now(),
        payload=common.EventPayload(common.EventPayloadType.SBS,
                                    common.SbsData('a', 'b', b'abc')))]

msgs = [
    common.PingMsg(),
    common.PongMsg(),
    common.InitMsg(client_id='client id',
                   client_token=None,
                   last_event_id=None,
                   subscriptions=[]),
    common.InitMsg(client_id='abc',
                   client_token='cba',
                   last_event_id=common.EventId(1, 2, 3),
                   subscriptions=[('a', 'b', 'c'), ('c', 'b', 'a')]),
    common.EventsMsg(events=[]),
    common.EventsMsg(events=events)]


@pytest.mark.parametrize('msg', msgs)
async def test_encode_decode(msg):
    encoded_msg = encoder.encode_msg(msg)
    decoded_msg = encoder.decode_msg(encoded_msg)

    assert decoded_msg == msg
