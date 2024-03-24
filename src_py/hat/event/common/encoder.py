import datetime
import struct

from hat import json
from hat import sbs
from hat import util

from hat.event.common.common import (Event,
                                     EventId,
                                     EventPayload,
                                     EventPayloadBinary,
                                     EventPayloadJson,
                                     Order,
                                     OrderBy,
                                     QueryLatestParams,
                                     QueryParams,
                                     QueryResult,
                                     QueryServerParams,
                                     QueryTimeseriesParams,
                                     RegisterEvent,
                                     Status,
                                     Timestamp)


def timestamp_to_bytes(t: Timestamp) -> util.Bytes:
    """Convert timestamp to 12 byte representation

    Bytes [0, 8] are big endian unsigned `Timestamp.s` + 2^63 and
    bytes [9, 12] are big endian unsigned `Timestamp.us`.

    """
    return struct.pack(">QI", t.s + (1 << 63), t.us)


def timestamp_from_bytes(data: util.Bytes) -> Timestamp:
    """Create new timestamp from 12 byte representation

    Bytes representation is same as defined for `timestamp_to_bytes` function.

    """
    s, us = struct.unpack(">QI", data)
    return Timestamp(s - (1 << 63), us)


def timestamp_to_float(t: Timestamp) -> float:
    """Convert timestamp to floating number of seconds since 1970-01-01 UTC

    For precise serialization see `timestamp_to_bytes`/`timestamp_from_bytes`.

    """
    return t.s + t.us * 1E-6


def timestamp_from_float(ts: float) -> Timestamp:
    """Create timestamp from floating number of seconds since 1970-01-01 UTC

    For precise serialization see `timestamp_to_bytes`/`timestamp_from_bytes`.

    """
    s = int(ts)
    if ts < 0:
        s = s - 1

    us = round((ts - s) * 1E6)

    if us == 1_000_000:
        return Timestamp(s + 1, 0)

    else:
        return Timestamp(s, us)


def timestamp_to_datetime(t: Timestamp) -> datetime.datetime:
    """Convert timestamp to datetime (representing utc time)

    For precise serialization see `timestamp_to_bytes`/`timestamp_from_bytes`.

    """
    try:
        dt_from_s = datetime.datetime.fromtimestamp(t.s, datetime.timezone.utc)

    except OSError:
        dt_from_s = (
            datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) +
            datetime.timedelta(seconds=t.s))

    return datetime.datetime(
        year=dt_from_s.year,
        month=dt_from_s.month,
        day=dt_from_s.day,
        hour=dt_from_s.hour,
        minute=dt_from_s.minute,
        second=dt_from_s.second,
        microsecond=t.us,
        tzinfo=datetime.timezone.utc)


def timestamp_from_datetime(dt: datetime.datetime) -> Timestamp:
    """Create new timestamp from datetime

    If `tzinfo` is not set, it is assumed that provided datetime represents
    utc time.

    For precise serialization see `timestamp_to_bytes`/`timestamp_from_bytes`.

    """
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=datetime.timezone.utc)

    s = int(dt.timestamp())

    if dt.timestamp() < 0:
        s = s - 1

    return Timestamp(s=s, us=dt.microsecond)


def timestamp_to_sbs(t: Timestamp) -> sbs.Data:
    """Convert timestamp to SBS data"""
    return {'s': t.s, 'us': t.us}


def timestamp_from_sbs(data: sbs.Data) -> Timestamp:
    """Create new timestamp from SBS data"""
    return Timestamp(s=data['s'], us=data['us'])


def status_to_sbs(status: Status) -> sbs.Data:
    """Convert Status to SBS data"""
    return status.value, None


def status_from_sbs(status: sbs.Data) -> Status:
    """Create Status based on SBS data"""
    return Status(status[0])


def event_to_sbs(event: Event) -> sbs.Data:
    """Convert Event to SBS data"""
    return {'id': _event_id_to_sbs(event.id),
            'type': list(event.type),
            'timestamp': timestamp_to_sbs(event.timestamp),
            'sourceTimestamp': _optional_to_sbs(event.source_timestamp,
                                                timestamp_to_sbs),
            'payload': _optional_to_sbs(event.payload, event_payload_to_sbs)}


def event_from_sbs(data: sbs.Data) -> Event:
    """Create Event based on SBS data"""
    return Event(id=_event_id_from_sbs(data['id']),
                 type=tuple(data['type']),
                 timestamp=timestamp_from_sbs(data['timestamp']),
                 source_timestamp=_optional_from_sbs(data['sourceTimestamp'],
                                                     timestamp_from_sbs),
                 payload=_optional_from_sbs(data['payload'],
                                            event_payload_from_sbs))


def register_event_to_sbs(event: RegisterEvent) -> sbs.Data:
    """Convert RegisterEvent to SBS data"""
    return {'type': list(event.type),
            'sourceTimestamp': _optional_to_sbs(event.source_timestamp,
                                                timestamp_to_sbs),
            'payload': _optional_to_sbs(event.payload, event_payload_to_sbs)}


def register_event_from_sbs(data: sbs.Data) -> RegisterEvent:
    """Create RegisterEvent based on SBS data"""
    return RegisterEvent(
        type=tuple(data['type']),
        source_timestamp=_optional_from_sbs(data['sourceTimestamp'],
                                            timestamp_from_sbs),
        payload=_optional_from_sbs(data['payload'], event_payload_from_sbs))


def query_params_to_sbs(params: QueryParams) -> sbs.Data:
    """Convert QueryParams to SBS data"""
    if isinstance(params, QueryLatestParams):
        return 'latest', {
            'eventTypes': _optional_to_sbs(params.event_types,
                                           _event_types_to_sbs)}

    if isinstance(params, QueryTimeseriesParams):
        return 'timeseries', {
            'eventTypes': _optional_to_sbs(params.event_types,
                                           _event_types_to_sbs),
            'tFrom': _optional_to_sbs(params.t_from, timestamp_to_sbs),
            'tTo': _optional_to_sbs(params.t_to, timestamp_to_sbs),
            'sourceTFrom': _optional_to_sbs(params.source_t_from,
                                            timestamp_to_sbs),
            'sourceTTo': _optional_to_sbs(params.source_t_to,
                                          timestamp_to_sbs),
            'order': (params.order.value, None),
            'orderBy': (params.order_by.value, None),
            'maxResults': _optional_to_sbs(params.max_results),
            'lastEventId': _optional_to_sbs(params.last_event_id,
                                            _event_id_to_sbs)}

    if isinstance(params, QueryServerParams):
        return 'server', {
            'serverId': params.server_id,
            'persisted': params.persisted,
            'maxResults': _optional_to_sbs(params.max_results),
            'lastEventId': _optional_to_sbs(params.last_event_id,
                                            _event_id_to_sbs)}

    raise ValueError('unsupported params type')


def query_params_from_sbs(data: sbs.Data) -> QueryParams:
    """Create QueryParams based on SBS data"""
    if data[0] == 'latest':
        return QueryLatestParams(
            event_types=_optional_from_sbs(data[1]['eventTypes'],
                                           _event_types_from_sbs))

    if data[0] == 'timeseries':
        return QueryTimeseriesParams(
            event_types=_optional_from_sbs(data[1]['eventTypes'],
                                           _event_types_from_sbs),
            t_from=_optional_from_sbs(data[1]['tFrom'], timestamp_from_sbs),
            t_to=_optional_from_sbs(data[1]['tTo'], timestamp_from_sbs),
            source_t_from=_optional_from_sbs(data[1]['sourceTFrom'],
                                             timestamp_from_sbs),
            source_t_to=_optional_from_sbs(data[1]['sourceTTo'],
                                           timestamp_from_sbs),
            order=Order(data[1]['order'][0]),
            order_by=OrderBy(data[1]['orderBy'][0]),
            max_results=_optional_from_sbs(data[1]['maxResults']),
            last_event_id=_optional_from_sbs(data[1]['lastEventId'],
                                             _event_id_from_sbs))

    if data[0] == 'server':
        return QueryServerParams(
            server_id=data[1]['serverId'],
            persisted=data[1]['persisted'],
            max_results=_optional_from_sbs(data[1]['maxResults']),
            last_event_id=_optional_from_sbs(data[1]['lastEventId'],
                                             _event_id_from_sbs))

    raise ValueError('unsupported params type')


def query_result_to_sbs(result: QueryResult) -> sbs.Data:
    """Convert QueryResult to SBS data"""
    return {'events': [event_to_sbs(event) for event in result.events],
            'moreFollows': result.more_follows}


def query_result_from_sbs(data: sbs.Data) -> QueryResult:
    """Create QueryResult based on SBS data"""
    return QueryResult(events=[event_from_sbs(i) for i in data['events']],
                       more_follows=data['moreFollows'])


def event_payload_to_sbs(payload: EventPayload) -> sbs.Data:
    """Convert EventPayload to SBS data"""
    if isinstance(payload, EventPayloadBinary):
        return 'binary', {'type': payload.type,
                          'data': payload.data}

    if isinstance(payload, EventPayloadJson):
        return 'json', json.encode(payload.data)

    raise ValueError('unsupported payload type')


def event_payload_from_sbs(data: sbs.Data) -> EventPayload:
    """Create EventPayload based on SBS data"""
    if data[0] == 'binary':
        return EventPayloadBinary(type=data[1]['type'],
                                  data=data[1]['data'])

    if data[0] == 'json':
        return EventPayloadJson(data=json.decode(data[1]))

    raise ValueError('unsupported payload type')


def _event_id_to_sbs(event_id):
    return {'server': event_id.server,
            'session': event_id.session,
            'instance': event_id.instance}


def _event_id_from_sbs(data):
    return EventId(server=data['server'],
                   session=data['session'],
                   instance=data['instance'])


def _event_types_to_sbs(event_types):
    return [list(event_type) for event_type in event_types]


def _event_types_from_sbs(data):
    return [tuple(i) for i in data]


def _optional_to_sbs(value, fn=lambda i: i):
    return ('value', fn(value)) if value is not None else ('none', None)


def _optional_from_sbs(data, fn=lambda i: i):
    return fn(data[1]) if data[0] == 'value' else None
