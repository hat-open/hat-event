import pytest

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder


@pytest.mark.parametrize('x', [
    0, 1, 1234, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF
])
def test_uint(x):
    encoded = encoder.encode_uint(x)
    decoded = encoder.decode_uint(encoded)
    assert x == decoded


@pytest.mark.parametrize('x', [
    common.now(),
    common.Timestamp(s=-(1 << 63), us=0),
    common.Timestamp(s=0, us=0),
    common.Timestamp(s=(1 << 63) - 1, us=int(1e6))
])
def test_timestamp(x):
    encoded = encoder.encode_timestamp(x)
    decoded = encoder.decode_timestamp(encoded)
    assert x == decoded


@pytest.mark.parametrize('x', [
    (),
    ('', ),
    ('a', 'b', 'c')
])
def test_tuple_str(x):
    encoded = encoder.encode_tuple_str(x)
    decoded = encoder.decode_tuple_str(encoded)
    assert x == decoded


@pytest.mark.parametrize('x', [
    [],
    [()],
    [('a',),  ('b',), ('c',)],
    [('a', 'b', 'c')]
])
def test_list_tuple_str(x):
    encoded = encoder.encode_list_tuple_str(x)
    decoded = encoder.decode_list_tuple_str(encoded)
    assert x == decoded


@pytest.mark.parametrize('x', [
    (123, common.now(), 321)
])
def test_uint_timestamp_uint(x):
    encoded = encoder.encode_uint_timestamp_uint(x)
    decoded = encoder.decode_uint_timestamp_uint(encoded)
    assert x == decoded


@pytest.mark.parametrize('x', [
    common.Event(event_id=common.EventId(1, 2),
                 event_type=('a', 'b', 'c'),
                 timestamp=common.now(),
                 source_timestamp=None,
                 payload=None)
])
def test_event(x):
    encoded = encoder.encode_event(x)
    decoded = encoder.decode_event(encoded)
    assert x == decoded
