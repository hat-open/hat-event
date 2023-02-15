import pytest

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder


@pytest.mark.parametrize('key', [
    0, 1, 1234, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF
])
def test_system_db_key(key):
    encoded = encoder.encode_system_db_key(key)
    decoded = encoder.decode_system_db_key(encoded)
    assert key == decoded


@pytest.mark.parametrize('event_id', [
    common.EventId(1, 2, 3),
    common.EventId(3, 2, 1),
    common.EventId(1, 2, 0xFFFF_FFFF),
    common.EventId(0xFFFF_FFFF, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF),
])
@pytest.mark.parametrize('ts', [
    common.now(),
    common.Timestamp(s=-(1 << 63), us=0),
    common.Timestamp(s=0, us=0),
    common.Timestamp(s=(1 << 63) - 1, us=int(1e6)),
])
def test_system_db_value(event_id, ts):
    value = (event_id, ts)
    encoded = encoder.encode_system_db_value(value)
    decoded = encoder.decode_system_db_value(encoded)
    assert value == decoded


@pytest.mark.parametrize('key', [
    0, 1, 1234, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF
])
def test_latest_data_db_key(key):
    encoded = encoder.encode_latest_data_db_key(key)
    decoded = encoder.decode_latest_data_db_key(encoded)
    assert key == decoded


@pytest.mark.parametrize('value', [
    common.Event(event_id=common.EventId(1, 2, 3),
                 event_type=('a', 'b', 'c'),
                 timestamp=common.now(),
                 source_timestamp=None,
                 payload=None),
    common.Event(event_id=common.EventId(1, 2, 3),
                 event_type=('abc', 'def'),
                 timestamp=common.now(),
                 source_timestamp=common.now(),
                 payload=common.EventPayload(type=common.EventPayloadType.JSON,
                                             data={'abc': 123, 'def': None}))
])
def test_latest_data_db_value(value):
    encoded = encoder.encode_latest_data_db_value(value)
    decoded = encoder.decode_latest_data_db_value(encoded)
    assert value == decoded


@pytest.mark.parametrize('key', [
    0, 1, 1234, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF
])
def test_latest_type_db_key(key):
    encoded = encoder.encode_latest_type_db_key(key)
    decoded = encoder.decode_latest_type_db_key(encoded)
    assert key == decoded


@pytest.mark.parametrize('value', [
    (),
    ('a', 'b', 'c'),
    ('abcdef', 'ghijklmnoprst'),
    tuple(str(i) for i in range(10)),
])
def test_latest_type_db_value(value):
    encoded = encoder.encode_latest_type_db_value(value)
    decoded = encoder.decode_latest_type_db_value(encoded)
    assert value == decoded


@pytest.mark.parametrize('partition_id', [
    0, 12345, 0xFFFF_FFFF
])
@pytest.mark.parametrize('ts', [
    common.now(),
    common.Timestamp(s=-(1 << 63), us=0),
    common.Timestamp(s=0, us=0),
    common.Timestamp(s=(1 << 63) - 1, us=int(1e6)),
])
@pytest.mark.parametrize('event_id', [
    common.EventId(1, 2, 3),
    common.EventId(3, 2, 1),
    common.EventId(1, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF),
])
def test_ordered_data_db_key(partition_id, ts, event_id):
    key = (partition_id, ts, event_id)
    encoded = encoder.encode_ordered_data_db_key(key)
    decoded = encoder.decode_ordered_data_db_key(encoded)
    assert key == decoded


@pytest.mark.parametrize('value', [
    common.Event(event_id=common.EventId(1, 2, 3),
                 event_type=('a', 'b', 'c'),
                 timestamp=common.now(),
                 source_timestamp=None,
                 payload=None),
    common.Event(event_id=common.EventId(1, 2, 3),
                 event_type=('abc', 'def'),
                 timestamp=common.now(),
                 source_timestamp=common.now(),
                 payload=common.EventPayload(type=common.EventPayloadType.JSON,
                                             data={'abc': 123, 'def': None}))
])
def test_ordered_data_db_value(value):
    encoded = encoder.encode_ordered_data_db_value(value)
    decoded = encoder.decode_ordered_data_db_value(encoded)
    assert value == decoded


@pytest.mark.parametrize('key', [
    0, 1, 1234, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF
])
def test_ordered_partition_db_key(key):
    encoded = encoder.encode_ordered_partition_db_key(key)
    decoded = encoder.decode_ordered_partition_db_key(encoded)
    assert key == decoded


@pytest.mark.parametrize('value', [
    0,
    None,
    'abc',
    {'abc': 123, 'def': 456},
])
def test_ordered_partition_db_value(value):
    encoded = encoder.encode_ordered_partition_db_value(value)
    decoded = encoder.decode_ordered_partition_db_value(encoded)
    assert value == decoded


@pytest.mark.parametrize('key', [
    0, 1, 1234, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF
])
def test_ordered_count_db_key(key):
    encoded = encoder.encode_ordered_count_db_key(key)
    decoded = encoder.decode_ordered_count_db_key(encoded)
    assert key == decoded


@pytest.mark.parametrize('value', [
    0, 1, 1234, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF
])
def test_ordered_count_db_value(value):
    encoded = encoder.encode_ordered_count_db_value(value)
    decoded = encoder.decode_ordered_count_db_value(encoded)
    assert value == decoded


@pytest.mark.parametrize('key', [
    common.EventId(1, 2, 3),
    common.EventId(3, 2, 1),
    common.EventId(1, 2, 0xFFFF_FFFF),
    common.EventId(0xFFFF_FFFF, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF),
])
def test_ref_db_key(key):
    encoded = encoder.encode_ref_db_key(key)
    decoded = encoder.decode_ref_db_key(encoded)
    assert key == decoded


@pytest.mark.parametrize('value', [
    set(),
    {common.LatestEventRef(key=1234)},
    {common.OrderedEventRef(key=(
        1234, common.now(), common.EventId(3, 2, 1)))},
    {common.LatestEventRef(key=1234),
     common.OrderedEventRef(key=(
        1234, common.now(), common.EventId(3, 2, 1)))},
])
def test_ref_db_value(value):
    encoded = encoder.encode_ref_db_value(value)
    decoded = encoder.decode_ref_db_value(encoded)
    assert value == decoded
