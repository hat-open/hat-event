from hat.event.common import *  # NOQA

from pathlib import Path
import enum
import itertools
import platform
import struct
import sys
import typing

import lmdb

from hat import json
from hat import util

from hat.event.common import (ServerId, Event, EventId, EventType,
                              Timestamp, EventPayloadBinary,
                              sbs_repo, event_to_sbs, event_from_sbs)


default_max_size: int = (1024 * 1024 * 1024 * 1024
                         if platform.architecture()[0] == '64bit'
                         else 2 * 1024 * 1024 * 1024)


DbKey = typing.TypeVar('DbKey')
DbValue = typing.TypeVar('DbValue')


class DbType(enum.Enum):
    SYSTEM_SETTINGS = 0
    SYSTEM_LAST_EVENT_ID = 1
    SYSTEM_LAST_TIMESTAMP = 2
    REF = 3
    LATEST_DATA = 4
    LATEST_TYPE = 5
    TIMESERIES_DATA = 6
    TIMESERIES_PARTITION = 7
    TIMESERIES_COUNT = 8


if sys.version_info[:2] >= (3, 11):

    class DbDef(typing.NamedTuple, typing.Generic[DbKey, DbValue]):
        encode_key: typing.Callable[[DbKey], util.Bytes]
        decode_key: typing.Callable[[util.Bytes], DbKey]
        encode_value: typing.Callable[[DbValue], util.Bytes]
        decode_value: typing.Callable[[util.Bytes], DbValue]

    def create_db_def(key_type: typing.Type,
                      value_type: typing.Type
                      ) -> typing.Type[DbDef]:
        return DbDef[key_type, value_type]

else:

    class DbDef(typing.NamedTuple):
        encode_key: typing.Callable[[DbKey], util.Bytes]
        decode_key: typing.Callable[[util.Bytes], DbKey]
        encode_value: typing.Callable[[DbValue], util.Bytes]
        decode_value: typing.Callable[[util.Bytes], DbValue]

    def create_db_def(key_type: typing.Type,
                      value_type: typing.Type
                      ) -> typing.Type[DbDef]:
        return DbDef


EventTypeRef: typing.TypeAlias = int
PartitionId: typing.TypeAlias = int
TimeseriesKey: typing.TypeAlias = tuple[PartitionId, Timestamp, EventId]


class SettingsId(enum.Enum):
    VERSION = 0
    IDENTIFIER = 1


class LatestEventRef(typing.NamedTuple):
    key: EventTypeRef


class TimeseriesEventRef(typing.NamedTuple):
    key: TimeseriesKey


EventRef: typing.TypeAlias = LatestEventRef | TimeseriesEventRef


db_defs = {
    DbType.SYSTEM_SETTINGS: create_db_def(SettingsId, json.Data)(
        encode_key=lambda key: _encode_uint(key.value),
        decode_key=lambda key_bytes: SettingsId(_decode_uint(key_bytes)),
        encode_value=lambda value: _encode_json(value),
        decode_value=lambda value_bytes: _decode_json(value_bytes)),

    DbType.SYSTEM_LAST_EVENT_ID: create_db_def(ServerId, EventId)(
        encode_key=lambda key: _encode_uint(key),
        decode_key=lambda key_bytes: _decode_uint(key_bytes),
        encode_value=lambda value: _encode_event_id(value),
        decode_value=lambda value_bytes: _decode_event_id(value_bytes)),

    DbType.SYSTEM_LAST_TIMESTAMP: create_db_def(ServerId, Timestamp)(
        encode_key=lambda key: _encode_uint(key),
        decode_key=lambda key_bytes: _decode_uint(key_bytes),
        encode_value=lambda value: _encode_timestamp(value),
        decode_value=lambda value_bytes: _decode_timestamp(value_bytes)),

    DbType.REF: create_db_def(EventId, set[EventRef])(
        encode_key=lambda key: _encode_event_id(key),
        decode_key=lambda key_bytes: _decode_event_id(key_bytes),
        encode_value=lambda value: bytes(_encode_event_refs(value)),
        decode_value=lambda value_bytes: set(_decode_event_refs(value_bytes))),

    DbType.LATEST_DATA: create_db_def(EventTypeRef, Event)(
        encode_key=lambda key: _encode_uint(key),
        decode_key=lambda key_bytes: _decode_uint(key_bytes),
        encode_value=lambda value: _encode_event(value),
        decode_value=lambda value_bytes: _decode_event(value_bytes)),

    DbType.LATEST_TYPE: create_db_def(EventTypeRef, EventType)(
        encode_key=lambda key: _encode_uint(key),
        decode_key=lambda key_bytes: _decode_uint(key_bytes),
        encode_value=lambda value: _encode_json(list(value)),
        decode_value=lambda value_bytes: tuple(_decode_json(value_bytes))),

    DbType.TIMESERIES_DATA: create_db_def(TimeseriesKey, Event)(
        encode_key=lambda key: _encode_timeseries_key(key),
        decode_key=lambda key_bytes: _decode_timeseries_key(key_bytes),
        encode_value=lambda value: _encode_event(value),
        decode_value=lambda value_bytes: _decode_event(value_bytes)),

    DbType.TIMESERIES_PARTITION: create_db_def(PartitionId, json.Data)(
        encode_key=lambda key: _encode_uint(key),
        decode_key=lambda key_bytes: _decode_uint(key_bytes),
        encode_value=lambda value: _encode_json(value),
        decode_value=lambda value_bytes: _decode_json(value_bytes)),

    DbType.TIMESERIES_COUNT: create_db_def(PartitionId, int)(
        encode_key=lambda key: _encode_uint(key),
        decode_key=lambda key_bytes: _decode_uint(key_bytes),
        encode_value=lambda value: _encode_uint(value),
        decode_value=lambda value_bytes: _decode_uint(value_bytes))}


def ext_create_env(path: Path,
                   max_size: int = default_max_size,
                   readonly: bool = False
                   ) -> lmdb.Environment:
    return lmdb.Environment(str(path),
                            map_size=max_size,
                            subdir=False,
                            max_dbs=len(DbType),
                            readonly=readonly)


def ext_open_db(env: lmdb.Environment,
                db_type: DbType,
                create: bool = True
                ) -> lmdb._Database:
    return env.open_db(db_type.name.encode('utf-8'), create=create)


def _encode_uint(value):
    return struct.pack(">Q", value)


def _decode_uint(value_bytes):
    return struct.unpack(">Q", value_bytes)[0]


def _encode_event_id(event_id):
    return struct.pack(">QQQ", event_id.server, event_id.session,
                       event_id.instance)


def _decode_event_id(event_id_bytes):
    server_id, session_id, instance_id = struct.unpack(">QQQ", event_id_bytes)
    return EventId(server=server_id,
                   session=session_id,
                   instance=instance_id)


def _encode_timestamp(timestamp):
    return struct.pack(">QI", timestamp.s + (1 << 63), timestamp.us)


def _decode_timestamp(timestamp_bytes):
    s, us = struct.unpack(">QI", timestamp_bytes)
    return Timestamp(s - (1 << 63), us)


def _encode_event_ref(ref):
    if isinstance(ref, LatestEventRef):
        db_type = DbType.LATEST_DATA

    elif isinstance(ref, TimeseriesEventRef):
        db_type = DbType.TIMESERIES_DATA

    else:
        raise ValueError('unsupported event reference')

    return bytes([db_type.value]) + db_defs[db_type].encode_key(ref.key)


def _decode_event_ref(ref_bytes):
    db_type = DbType(ref_bytes[0])
    key = db_defs[db_type].decode_key(ref_bytes[1:])

    if db_type == DbType.LATEST_DATA:
        return LatestEventRef(key)

    if db_type == DbType.TIMESERIES_DATA:
        return TimeseriesEventRef(key)

    raise ValueError('unsupported database type')


def _encode_event_refs(refs):
    return bytes(itertools.chain.from_iterable(_encode_event_ref(ref)
                                               for ref in refs))


def _decode_event_refs(refs_bytes):
    while refs_bytes:
        db_type = DbType(refs_bytes[0])

        if db_type == DbType.LATEST_DATA:
            ref_key_len = 8

        elif db_type == DbType.TIMESERIES_DATA:
            ref_key_len = 44

        else:
            raise ValueError('unsupported event reference')

        ref, refs_bytes = (_decode_event_ref(refs_bytes[:ref_key_len+1]),
                           refs_bytes[ref_key_len+1:])
        yield ref


def _encode_timeseries_key(key):
    partition_id, timestamp, event_id = key
    return bytes(itertools.chain(_encode_uint(partition_id),
                                 _encode_timestamp(timestamp),
                                 _encode_event_id(event_id)))


def _decode_timeseries_key(key_bytes):
    partition_id = _decode_uint(key_bytes[:8])
    timestamp = _decode_timestamp(key_bytes[8:20])
    event_id = _decode_event_id(key_bytes[20:])
    return partition_id, timestamp, event_id


def _encode_event(event):
    event_sbs = event_to_sbs(event)
    return sbs_repo.encode('HatEventer.Event', event_sbs)


def _decode_event(event_bytes):
    event_sbs = sbs_repo.decode('HatEventer.Event', event_bytes)
    event = event_from_sbs(event_sbs)

    if isinstance(event.payload, EventPayloadBinary):
        event = event._replace(
            payload=event.payload._replace(data=bytes(event.payload.data)))

    return event


def _encode_json(data):
    return json.encode(data).encode('utf-8')


def _decode_json(data_bytes):
    return json.decode(str(data_bytes, encoding='utf-8'))
