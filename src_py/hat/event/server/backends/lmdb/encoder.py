import itertools
import struct

from hat import json
from hat.event.server.backends.lmdb import common


def encode_system_db_key(key: common.SystemDbKey) -> bytes:
    return _encode_uint(key)


def decode_system_db_key(key_bytes: bytes) -> common.SystemDbKey:
    key, _ = _decode_uint(key_bytes)
    return key


def encode_system_db_value(value: common.SystemDbValue) -> bytes:
    event_id, timestamp = value
    return _encode_event_id(event_id) + _encode_timestamp(timestamp)


def decode_system_db_value(value_bytes: bytes) -> common.SystemDbValue:
    event_id, rest = _decode_event_id(value_bytes)
    timestamp, _ = _decode_timestamp(rest)
    return event_id, timestamp


def encode_latest_data_db_key(key: common.LatestDataDbKey) -> bytes:
    return _encode_uint(key)


def decode_latest_data_db_key(key_bytes: bytes) -> common.LatestDataDbKey:
    key, _ = _decode_uint(key_bytes)
    return key


def encode_latest_data_db_value(value: common.LatestDataDbValue) -> bytes:
    return _encode_event(value)


def decode_latest_data_db_value(value_bytes: bytes
                                ) -> common.LatestDataDbValue:
    return _decode_event(value_bytes)


def encode_latest_type_db_key(key: common.LatestTypeDbKey) -> bytes:
    return _encode_uint(key)


def decode_latest_type_db_key(key_bytes: bytes) -> common.LatestTypeDbKey:
    key, _ = _decode_uint(key_bytes)
    return key


def encode_latest_type_db_value(value: common.OrderedDataDbKey) -> bytes:
    return _encode_json(list(value))


def decode_latest_type_db_value(value_bytes: bytes) -> common.OrderedDataDbKey:
    return tuple(_decode_json(value_bytes))


def encode_ordered_data_db_key(key: common.OrderedDataDbKey) -> bytes:
    partition_id, timestamp, event_id = key
    return (_encode_uint(partition_id) +
            _encode_timestamp(timestamp) +
            _encode_event_id(event_id))


def decode_ordered_data_db_key(key_bytes: bytes) -> common.OrderedDataDbKey:
    partition_id, rest = _decode_uint(key_bytes)
    timestamp, rest = _decode_timestamp(rest)
    event_id, _ = _decode_event_id(rest)
    return partition_id, timestamp, event_id


def encode_ordered_data_db_value(value: common.OrderedDataDbValue) -> bytes:
    return _encode_event(value)


def decode_ordered_data_db_value(value_bytes: bytes
                                 ) -> common.OrderedDataDbValue:
    return _decode_event(value_bytes)


def encode_ordered_partition_db_key(key: common.OrderedPartitionDbKey
                                    ) -> bytes:
    return _encode_uint(key)


def decode_ordered_partition_db_key(key_bytes: bytes
                                    ) -> common.OrderedPartitionDbKey:
    key, _ = _decode_uint(key_bytes)
    return key


def encode_ordered_partition_db_value(value: common.OrderedPartitionDbValue
                                      ) -> bytes:
    return _encode_json(value)


def decode_ordered_partition_db_value(value_bytes: bytes
                                      ) -> common.OrderedPartitionDbValue:
    return _decode_json(value_bytes)


def encode_ordered_count_db_key(key: common.OrderedCountDbKey) -> bytes:
    return _encode_uint(key)


def decode_ordered_count_db_key(key_bytes: bytes) -> common.OrderedCountDbKey:
    key, _ = _decode_uint(key_bytes)
    return key


def encode_ordered_count_db_value(value: common.OrderedCountDbValue) -> bytes:
    return _encode_uint(value)


def decode_ordered_count_db_value(value_bytes: bytes
                                  ) -> common.OrderedCountDbValue:
    value, _ = _decode_uint(value_bytes)
    return value


def encode_ref_db_key(key: common.RefDbKey) -> bytes:
    return _encode_event_id(key)


def decode_ref_db_key(key_bytes: bytes) -> common.RefDbKey:
    event_id, _ = _decode_event_id(key_bytes)
    return event_id


def encode_ref_db_value(value: common.RefDbValue) -> bytes:
    return bytes(itertools.chain.from_iterable(
        _encode_event_ref(ref) for ref in value))


def decode_ref_db_value(value_bytes: bytes) -> common.RefDbValue:
    refs = set()
    while value_bytes:
        ref, value_bytes = _decode_event_ref(value_bytes)
        refs.add(ref)
    return refs


def _encode_uint(value):
    return struct.pack(">Q", value)


def _decode_uint(value_bytes):
    return struct.unpack(">Q", value_bytes[:8])[0], value_bytes[8:]


def _encode_event_id(event_id):
    return struct.pack(">QQQ", event_id.server, event_id.session,
                       event_id.instance)


def _decode_event_id(event_id_bytes):
    server_id, session_id, instance_id = struct.unpack(">QQQ",
                                                       event_id_bytes[:24])
    event_id = common.EventId(server=server_id,
                              session=session_id,
                              instance=instance_id)
    return event_id, event_id_bytes[24:]


def _encode_timestamp(timestamp):
    return struct.pack(">QI", timestamp.s + (1 << 63), timestamp.us)


def _decode_timestamp(timestamp_bytes):
    s, us = struct.unpack(">QI", timestamp_bytes[:12])
    return common.Timestamp(s - (1 << 63), us), timestamp_bytes[12:]


def _encode_event_ref(ref):
    if isinstance(ref, common.LatestEventRef):
        yield common.DbType.LATEST_DATA.value
        yield from encode_latest_data_db_key(ref.key)

    elif isinstance(ref, common.OrderedEventRef):
        yield common.DbType.ORDERED_DATA.value
        yield from encode_ordered_data_db_key(ref.key)

    else:
        raise ValueError('unsupported event reference')


def _decode_event_ref(ref_bytes):
    db_type, rest = common.DbType(ref_bytes[0]), ref_bytes[1:]

    if db_type == common.DbType.LATEST_DATA:
        ref = common.LatestEventRef(decode_latest_data_db_key(rest[:8]))
        return ref, rest[8:]

    if db_type == common.DbType.ORDERED_DATA:
        ref = common.OrderedEventRef(decode_ordered_data_db_key(rest[:44]))
        return ref, rest[44:]

    raise ValueError('unsupported database type')


def _encode_event(event):
    event_sbs = common.event_to_sbs(event)
    return common.sbs_repo.encode('HatEventer', 'Event', event_sbs)


def _decode_event(event_bytes):
    event_sbs = common.sbs_repo.decode('HatEventer', 'Event', event_bytes)
    return common.event_from_sbs(event_sbs)


def _encode_json(data):
    return json.encode(data).encode('utf-8')


def _decode_json(data_bytes):
    return json.decode(str(data_bytes, encoding='utf-8'))
