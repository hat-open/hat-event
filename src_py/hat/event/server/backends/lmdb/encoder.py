import struct
import typing

from hat import json
from hat.event.server.backends.lmdb import common


def encode_uint(x: int) -> bytes:
    return struct.pack(">Q", x)


def decode_uint(x: bytes) -> int:
    return struct.unpack(">Q", x)[0]


def encode_timestamp(x: common.Timestamp) -> bytes:
    return struct.pack(">QI", x.s + (1 << 63), x.us)


def decode_timestamp(x: bytes) -> common.Timestamp:
    res = struct.unpack(">QI", x)
    return common.Timestamp(res[0] - (1 << 63), res[1])


def encode_tuple_str(x: typing.Tuple[str, ...]) -> bytes:
    return json.encode(list(x)).encode('utf-8')


def decode_tuple_str(x: bytes) -> typing.Tuple[str, ...]:
    return tuple(json.decode(str(x, encoding='utf-8')))


def encode_json(x: json.Data) -> bytes:
    return json.encode(x).encode('utf-8')


def decode_json(x: bytes) -> json.Data:
    return json.decode(str(x, encoding='utf-8'))


def encode_uint_timestamp_uint(x: typing.Tuple[int, common.Timestamp, int]
                               ) -> bytes:
    return struct.pack(">QQIQ", x[0], x[1].s + (1 << 63), x[1].us, x[2])


def decode_uint_timestamp_uint(x: bytes
                               ) -> typing.Tuple[int, common.Timestamp, int]:
    res = struct.unpack(">QQIQ", x)
    return res[0], common.Timestamp(res[1] - (1 << 63), res[2]), res[3]


def encode_event(event: common.Event) -> bytes:
    event_sbs = common.event_to_sbs(event)
    return common.sbs_repo.encode('HatEvent', 'Event', event_sbs)


def decode_event(event_bytes: bytes) -> common.Event:
    event_sbs = common.sbs_repo.decode('HatEvent', 'Event', event_bytes)
    return common.event_from_sbs(event_sbs)
