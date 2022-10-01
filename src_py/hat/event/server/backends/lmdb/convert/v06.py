from pathlib import Path
import enum
import struct
import platform
import typing

import lmdb

from hat import chatter
from hat import json
from hat import sbs


EventType: typing.Type = typing.Tuple[str, ...]
"""Event type"""


EventPayloadType = enum.Enum('EventPayloadType', [
    'BINARY',
    'JSON',
    'SBS'])


class EventId(typing.NamedTuple):
    server: int
    """server identifier"""
    instance: int
    """event instance identifier"""


class SbsData(typing.NamedTuple):
    module: typing.Optional[str]
    """SBS module name"""
    type: str
    """SBS type name"""
    data: bytes


class EventPayload(typing.NamedTuple):
    type: EventPayloadType
    data: typing.Union[bytes, json.Data, SbsData]


class Timestamp(typing.NamedTuple):
    s: int
    """seconds since 1970-01-01 (can be negative)"""
    us: int
    """microseconds added to timestamp seconds in range [0, 1e6)"""

    def __lt__(self, other):
        if not isinstance(other, Timestamp):
            return NotImplemented
        return self.s * 1000000 + self.us < other.s * 1000000 + other.us

    def __gt__(self, other):
        if not isinstance(other, Timestamp):
            return NotImplemented
        return self.s * 1000000 + self.us > other.s * 1000000 + other.us

    def __eq__(self, other):
        if not isinstance(other, Timestamp):
            return NotImplemented
        return self.s * 1000000 + self.us == other.s * 1000000 + other.us

    def __ne__(self, other):
        return not self == other

    def __le__(self, other):
        return self < other or self == other

    def __ge__(self, other):
        return self > other or self == other

    def __hash__(self):
        return self.s * 1000000 + self.us


class Event(typing.NamedTuple):
    event_id: EventId
    event_type: EventType
    timestamp: Timestamp
    source_timestamp: typing.Optional[Timestamp]
    payload: typing.Optional[EventPayload]


def decode_uint(x: bytes) -> int:
    return struct.unpack(">Q", x)[0]


def decode_timestamp(x: bytes) -> Timestamp:
    res = struct.unpack(">QI", x)
    return Timestamp(res[0] - (1 << 63), res[1])


def decode_tuple_str(x: bytes) -> typing.Tuple[str, ...]:
    return tuple(json.decode(str(x, encoding='utf-8')))


def decode_json(x: bytes) -> json.Data:
    return json.decode(str(x, encoding='utf-8'))


def decode_uint_timestamp_uint(x: bytes
                               ) -> typing.Tuple[int, Timestamp, int]:
    res = struct.unpack(">QQIQ", x)
    return res[0], Timestamp(res[1] - (1 << 63), res[2]), res[3]


def decode_event(event_bytes: bytes) -> Event:
    event_sbs = _sbs_repo.decode('HatEvent', 'Event', event_bytes)
    return _event_from_sbs(event_sbs)


def create_env(path: Path):
    max_dbs = 5
    max_db_size = (512 * 1024 * 1024 * 1024
                   if platform.architecture()[0] == '64bit'
                   else 1024 * 1024 * 1024)
    return lmdb.Environment(str(path),
                            map_size=max_db_size,
                            subdir=False,
                            max_dbs=max_dbs)


_sbs_repo = sbs.Repository(chatter.sbs_repo, r"""
module HatEvent

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
    sbs:     Hat.Data
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


def _event_from_sbs(data: sbs.Data) -> Event:
    return Event(
        event_id=_event_id_from_sbs(data['id']),
        event_type=tuple(data['type']),
        timestamp=_timestamp_from_sbs(data['timestamp']),
        source_timestamp=_optional_from_sbs(data['sourceTimestamp'],
                                            _timestamp_from_sbs),
        payload=_optional_from_sbs(data['payload'], _event_payload_from_sbs))


def _event_payload_from_sbs(data: sbs.Data) -> EventPayload:
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


def _timestamp_from_sbs(data: sbs.Data) -> Timestamp:
    return Timestamp(s=data['s'], us=data['us'])


def _event_id_from_sbs(data: sbs.Data) -> EventId:
    return EventId(server=data['server'],
                   instance=data['instance'])


def _sbs_data_from_sbs(data: sbs.Data) -> SbsData:
    return SbsData(module=_optional_from_sbs(data['module']),
                   type=data['type'],
                   data=data['data'])


def _optional_from_sbs(data: sbs.Data,
                       fn=lambda i: i
                       ) -> typing.Optional[typing.Any]:
    return fn(data[1]) if data[0] == 'value' else None
