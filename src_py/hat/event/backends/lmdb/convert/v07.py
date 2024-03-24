from pathlib import Path
import enum
import itertools
import platform
import struct
import typing

import lmdb

from hat import json
from hat import sbs


OrderBy = enum.Enum('OrderBy', [
    'TIMESTAMP',
    'SOURCE_TIMESTAMP'])


EventType: typing.TypeAlias = typing.Tuple[str, ...]


class EventId(typing.NamedTuple):
    server: int
    session: int
    instance: int


EventPayloadType = enum.Enum('EventPayloadType', [
    'BINARY',
    'JSON',
    'SBS'])


class SbsData(typing.NamedTuple):
    module: str | None
    type: str
    data: bytes


class EventPayload(typing.NamedTuple):
    type: EventPayloadType
    data: bytes | json.Data | SbsData


class Timestamp(typing.NamedTuple):
    s: int
    us: int

    def add(self, s: float) -> 'Timestamp':
        us = self.us + round((s - int(s)) * 1e6)
        s = self.s + int(s)
        return Timestamp(s=s + us // int(1e6),
                         us=us % int(1e6))


class Event(typing.NamedTuple):
    event_id: EventId
    event_type: EventType
    timestamp: Timestamp
    source_timestamp: Timestamp | None
    payload: EventPayload | None


class DbType(enum.Enum):
    SYSTEM = 0
    LATEST_DATA = 1
    LATEST_TYPE = 2
    ORDERED_DATA = 3
    ORDERED_PARTITION = 4
    ORDERED_COUNT = 5
    REF = 6


ServerId: typing.TypeAlias = int
EventTypeRef: typing.TypeAlias = int
PartitionId: typing.TypeAlias = int


class LatestEventRef(typing.NamedTuple):
    key: 'LatestDataDbKey'


class OrderedEventRef(typing.NamedTuple):
    key: 'OrderedDataDbKey'


EventRef: typing.TypeAlias = LatestEventRef | OrderedEventRef

SystemDbKey = ServerId
SystemDbValue = tuple[EventId, Timestamp]

LatestDataDbKey = EventTypeRef
LatestDataDbValue = Event

LatestTypeDbKey = EventTypeRef
LatestTypeDbValue = EventType

OrderedDataDbKey = tuple[PartitionId, Timestamp, EventId]
OrderedDataDbValue = Event

OrderedPartitionDbKey = PartitionId
OrderedPartitionDbValue = json.Data

OrderedCountDbKey = PartitionId
OrderedCountDbValue = int

RefDbKey = EventId
RefDbValue = set[EventRef]


def encode_system_db_key(key: SystemDbKey) -> bytes:
    return _encode_uint(key)


def decode_system_db_key(key_bytes: bytes) -> SystemDbKey:
    key, _ = _decode_uint(key_bytes)
    return key


def encode_system_db_value(value: SystemDbValue) -> bytes:
    event_id, timestamp = value
    return _encode_event_id(event_id) + _encode_timestamp(timestamp)


def decode_system_db_value(value_bytes: bytes) -> SystemDbValue:
    event_id, rest = _decode_event_id(value_bytes)
    timestamp, _ = _decode_timestamp(rest)
    return event_id, timestamp


def encode_latest_data_db_key(key: LatestDataDbKey) -> bytes:
    return _encode_uint(key)


def decode_latest_data_db_key(key_bytes: bytes) -> LatestDataDbKey:
    key, _ = _decode_uint(key_bytes)
    return key


def encode_latest_data_db_value(value: LatestDataDbValue) -> bytes:
    return _encode_event(value)


def decode_latest_data_db_value(value_bytes: bytes) -> LatestDataDbValue:
    return _decode_event(value_bytes)


def encode_latest_type_db_key(key: LatestTypeDbKey) -> bytes:
    return _encode_uint(key)


def decode_latest_type_db_key(key_bytes: bytes) -> LatestTypeDbKey:
    key, _ = _decode_uint(key_bytes)
    return key


def encode_latest_type_db_value(value: OrderedDataDbKey) -> bytes:
    return _encode_json(list(value))


def decode_latest_type_db_value(value_bytes: bytes) -> OrderedDataDbKey:
    return tuple(_decode_json(value_bytes))


def encode_ordered_data_db_key(key: OrderedDataDbKey) -> bytes:
    partition_id, timestamp, event_id = key
    return (_encode_uint(partition_id) +
            _encode_timestamp(timestamp) +
            _encode_event_id(event_id))


def decode_ordered_data_db_key(key_bytes: bytes) -> OrderedDataDbKey:
    partition_id, rest = _decode_uint(key_bytes)
    timestamp, rest = _decode_timestamp(rest)
    event_id, _ = _decode_event_id(rest)
    return partition_id, timestamp, event_id


def encode_ordered_data_db_value(value: OrderedDataDbValue) -> bytes:
    return _encode_event(value)


def decode_ordered_data_db_value(value_bytes: bytes) -> OrderedDataDbValue:
    return _decode_event(value_bytes)


def encode_ordered_partition_db_key(key: OrderedPartitionDbKey) -> bytes:
    return _encode_uint(key)


def decode_ordered_partition_db_key(key_bytes: bytes) -> OrderedPartitionDbKey:
    key, _ = _decode_uint(key_bytes)
    return key


def encode_ordered_partition_db_value(value: OrderedPartitionDbValue) -> bytes:
    return _encode_json(value)


def decode_ordered_partition_db_value(value_bytes: bytes
                                      ) -> OrderedPartitionDbValue:
    return _decode_json(value_bytes)


def encode_ordered_count_db_key(key: OrderedCountDbKey) -> bytes:
    return _encode_uint(key)


def decode_ordered_count_db_key(key_bytes: bytes) -> OrderedCountDbKey:
    key, _ = _decode_uint(key_bytes)
    return key


def encode_ordered_count_db_value(value: OrderedCountDbValue) -> bytes:
    return _encode_uint(value)


def decode_ordered_count_db_value(value_bytes: bytes) -> OrderedCountDbValue:
    value, _ = _decode_uint(value_bytes)
    return value


def encode_ref_db_key(key: RefDbKey) -> bytes:
    return _encode_event_id(key)


def decode_ref_db_key(key_bytes: bytes) -> RefDbKey:
    event_id, _ = _decode_event_id(key_bytes)
    return event_id


def encode_ref_db_value(value: RefDbValue) -> bytes:
    return bytes(itertools.chain.from_iterable(
        _encode_event_ref(ref) for ref in value))


def decode_ref_db_value(value_bytes: bytes) -> RefDbValue:
    refs = set()
    while value_bytes:
        ref, value_bytes = _decode_event_ref(value_bytes)
        refs.add(ref)
    return refs


def open_db(env: lmdb.Environment,
            db_type: DbType
            ) -> lmdb._Database:
    return env.open_db(db_type.name.encode('utf-8'))


def create_env(path: Path) -> lmdb.Environment:
    max_dbs = len(DbType)
    max_db_size = (512 * 1024 * 1024 * 1024
                   if platform.architecture()[0] == '64bit'
                   else 1024 * 1024 * 1024)
    return lmdb.Environment(str(path),
                            map_size=max_db_size,
                            subdir=False,
                            max_dbs=max_dbs)


_sbs_repo = sbs.Repository(r"""
module HatEventer

MsgSubscribe = Array(EventType)

MsgNotify = Array(Event)

MsgRegisterReq = Array(RegisterEvent)

MsgRegisterRes = Array(Choice {
    event:    Event
    failure:  None
})

MsgQueryReq = QueryData

MsgQueryRes = Array(Event)

Timestamp = Record {
    s:   Integer
    us:  Integer
}

EventId = Record {
    server:    Integer
    session:   Integer
    instance:  Integer
}

Order = Choice {
    descending:  None
    ascending:   None
}

OrderBy = Choice {
    timestamp:        None
    sourceTimestamp:  None
}

EventType = Array(String)

EventPayload = Choice {
    binary:  Bytes
    json:    String
    sbs:     Record {
        module:  Optional(String)
        type:    String
        data:    Bytes
    }
}

Event = Record {
    id:               EventId
    type:             EventType
    timestamp:        Timestamp
    sourceTimestamp:  Optional(Timestamp)
    payload:          Optional(EventPayload)
}

RegisterEvent = Record {
    type:             EventType
    sourceTimestamp:  Optional(Timestamp)
    payload:          Optional(EventPayload)
}

QueryData = Record {
    serverId:           Optional(Integer)
    ids:                Optional(Array(EventId))
    types:              Optional(Array(EventType))
    tFrom:              Optional(Timestamp)
    tTo:                Optional(Timestamp)
    sourceTFrom:        Optional(Timestamp)
    sourceTTo:          Optional(Timestamp)
    payload:            Optional(EventPayload)
    order:              Order
    orderBy:            OrderBy
    uniqueType:         Boolean
    maxResults:         Optional(Integer)
}
""")


def _event_to_sbs(event):
    return {
        'id': _event_id_to_sbs(event.event_id),
        'type': list(event.event_type),
        'timestamp': _timestamp_to_sbs(event.timestamp),
        'sourceTimestamp': _optional_to_sbs(event.source_timestamp,
                                            _timestamp_to_sbs),
        'payload': _optional_to_sbs(event.payload, _event_payload_to_sbs)}


def _event_from_sbs(data):
    return Event(
        event_id=_event_id_from_sbs(data['id']),
        event_type=tuple(data['type']),
        timestamp=_timestamp_from_sbs(data['timestamp']),
        source_timestamp=_optional_from_sbs(data['sourceTimestamp'],
                                            _timestamp_from_sbs),
        payload=_optional_from_sbs(data['payload'], _event_payload_from_sbs))


def _event_id_to_sbs(event_id):
    return {'server': event_id.server,
            'session': event_id.session,
            'instance': event_id.instance}


def _event_id_from_sbs(data):
    return EventId(server=data['server'],
                   session=data['session'],
                   instance=data['instance'])


def _timestamp_to_sbs(t):
    return {'s': t.s, 'us': t.us}


def _timestamp_from_sbs(data):
    return Timestamp(s=data['s'], us=data['us'])


def _event_payload_to_sbs(payload):
    if payload.type == EventPayloadType.BINARY:
        return 'binary', payload.data

    if payload.type == EventPayloadType.JSON:
        return 'json', json.encode(payload.data)

    if payload.type == EventPayloadType.SBS:
        return 'sbs', _sbs_data_to_sbs(payload.data)

    raise ValueError('unsupported payload type')


def _event_payload_from_sbs(data):
    data_type, data_data = data

    if data_type == 'binary':
        return EventPayload(type=EventPayloadType.BINARY,
                            data=data_data)

    if data_type == 'json':
        return EventPayload(type=EventPayloadType.JSON,
                            data=json.decode(data_data))

    if data_type == 'sbs':
        return EventPayload(type=EventPayloadType.SBS,
                            data=_sbs_data_from_sbs(data_data))

    raise ValueError('unsupported payload type')


def _sbs_data_to_sbs(data):
    return {'module': _optional_to_sbs(data.module),
            'type': data.type,
            'data': data.data}


def _sbs_data_from_sbs(data):
    return SbsData(module=_optional_from_sbs(data['module']),
                   type=data['type'],
                   data=data['data'])


def _optional_to_sbs(value, fn=lambda i: i):
    return ('value', fn(value)) if value is not None else ('none', None)


def _optional_from_sbs(data, fn=lambda i: i):
    return fn(data[1]) if data[0] == 'value' else None


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
    event_id = EventId(server=server_id,
                       session=session_id,
                       instance=instance_id)
    return event_id, event_id_bytes[24:]


def _encode_timestamp(timestamp):
    return struct.pack(">QI", timestamp.s + (1 << 63), timestamp.us)


def _decode_timestamp(timestamp_bytes):
    s, us = struct.unpack(">QI", timestamp_bytes[:12])
    return Timestamp(s - (1 << 63), us), timestamp_bytes[12:]


def _encode_event(event):
    event_sbs = _event_to_sbs(event)
    return _sbs_repo.encode('HatEventer.Event', event_sbs)


def _decode_event(event_bytes):
    event_sbs = _sbs_repo.decode('HatEventer.Event', event_bytes)
    return _event_from_sbs(event_sbs)


def _encode_json(data):
    return json.encode(data).encode('utf-8')


def _decode_json(data_bytes):
    return json.decode(str(data_bytes, encoding='utf-8'))


def _encode_event_ref(ref):
    if isinstance(ref, LatestEventRef):
        yield DbType.LATEST_DATA.value
        yield from encode_latest_data_db_key(ref.key)

    elif isinstance(ref, OrderedEventRef):
        yield DbType.ORDERED_DATA.value
        yield from encode_ordered_data_db_key(ref.key)

    else:
        raise ValueError('unsupported event reference')


def _decode_event_ref(ref_bytes):
    db_type, rest = DbType(ref_bytes[0]), ref_bytes[1:]

    if db_type == DbType.LATEST_DATA:
        ref = LatestEventRef(decode_latest_data_db_key(rest[:8]))
        return ref, rest[8:]

    if db_type == DbType.ORDERED_DATA:
        ref = OrderedEventRef(decode_ordered_data_db_key(rest[:44]))
        return ref, rest[44:]

    raise ValueError('unsupported database type')
