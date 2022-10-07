import enum
import importlib.resources
import typing

from hat import chatter
from hat import json
from hat import sbs
from hat.event.common.timestamp import (Timestamp,
                                        timestamp_to_sbs,
                                        timestamp_from_sbs)
import hat.monitor.common


with importlib.resources.path(__package__, 'json_schema_repo.json') as _path:
    json_schema_repo: json.SchemaRepository = json.SchemaRepository(
        json.json_schema_repo,
        hat.monitor.common.json_schema_repo,
        json.SchemaRepository.from_json(_path))
    """JSON schema repository"""

with importlib.resources.path(__package__, 'sbs_repo.json') as _path:
    sbs_repo: sbs.Repository = sbs.Repository(chatter.sbs_repo,
                                              sbs.Repository.from_json(_path))
    """SBS schema repository"""

EventType: typing.Type = typing.Tuple[str, ...]
"""Event type"""


Order = enum.Enum('Order', [
    'DESCENDING',
    'ASCENDING'])


OrderBy = enum.Enum('OrderBy', [
    'TIMESTAMP',
    'SOURCE_TIMESTAMP'])


EventPayloadType = enum.Enum('EventPayloadType', [
    'BINARY',
    'JSON',
    'SBS'])


class EventId(typing.NamedTuple):
    server: int
    """server identifier"""
    session: int
    """session identifier"""
    instance: int
    """event instance identifier"""


class EventPayload(typing.NamedTuple):
    type: EventPayloadType
    data: typing.Union[bytes, json.Data, 'SbsData']


class SbsData(typing.NamedTuple):
    module: typing.Optional[str]
    """SBS module name"""
    type: str
    """SBS type name"""
    data: bytes


class Event(typing.NamedTuple):
    event_id: EventId
    event_type: EventType
    timestamp: 'Timestamp'
    source_timestamp: typing.Optional['Timestamp']
    payload: typing.Optional[EventPayload]


class RegisterEvent(typing.NamedTuple):
    event_type: EventType
    source_timestamp: typing.Optional['Timestamp']
    payload: typing.Optional[EventPayload]


class QueryData(typing.NamedTuple):
    server_id: typing.Optional[int] = None
    event_ids: typing.Optional[typing.List[EventId]] = None
    event_types: typing.Optional[typing.List[EventType]] = None
    t_from: typing.Optional['Timestamp'] = None
    t_to: typing.Optional['Timestamp'] = None
    source_t_from: typing.Optional['Timestamp'] = None
    source_t_to: typing.Optional['Timestamp'] = None
    payload: typing.Optional[EventPayload] = None
    order: Order = Order.DESCENDING
    order_by: OrderBy = OrderBy.TIMESTAMP
    unique_type: bool = False
    max_results: typing.Optional[int] = None


class SyncerReq(typing.NamedTuple):
    last_event_id: EventId
    client_name: str


def event_to_sbs(event: Event) -> sbs.Data:
    """Convert Event to SBS data"""
    return {
        'id': _event_id_to_sbs(event.event_id),
        'type': list(event.event_type),
        'timestamp': timestamp_to_sbs(event.timestamp),
        'sourceTimestamp': _optional_to_sbs(event.source_timestamp,
                                            timestamp_to_sbs),
        'payload': _optional_to_sbs(event.payload, event_payload_to_sbs)}


def event_from_sbs(data: sbs.Data) -> Event:
    """Create new Event based on SBS data"""
    return Event(
        event_id=_event_id_from_sbs(data['id']),
        event_type=tuple(data['type']),
        timestamp=timestamp_from_sbs(data['timestamp']),
        source_timestamp=_optional_from_sbs(data['sourceTimestamp'],
                                            timestamp_from_sbs),
        payload=_optional_from_sbs(data['payload'], event_payload_from_sbs))


def register_event_to_sbs(event: RegisterEvent) -> sbs.Data:
    """Convert RegisterEvent to SBS data"""
    return {
        'type': list(event.event_type),
        'sourceTimestamp': _optional_to_sbs(event.source_timestamp,
                                            timestamp_to_sbs),
        'payload': _optional_to_sbs(event.payload, event_payload_to_sbs)}


def register_event_from_sbs(data: sbs.Data) -> RegisterEvent:
    """Create new RegisterEvent based on SBS data"""
    return RegisterEvent(
        event_type=tuple(data['type']),
        source_timestamp=_optional_from_sbs(data['sourceTimestamp'],
                                            timestamp_from_sbs),
        payload=_optional_from_sbs(data['payload'], event_payload_from_sbs))


def query_to_sbs(query: QueryData) -> sbs.Data:
    """Convert QueryData to SBS data"""
    return {
        'serverId': _optional_to_sbs(query.server_id),
        'ids': _optional_to_sbs(query.event_ids, lambda ids: [
            _event_id_to_sbs(i) for i in ids]),
        'types': _optional_to_sbs(query.event_types, lambda ets: [
            list(et) for et in ets]),
        'tFrom': _optional_to_sbs(query.t_from, timestamp_to_sbs),
        'tTo': _optional_to_sbs(query.t_to, timestamp_to_sbs),
        'sourceTFrom': _optional_to_sbs(query.source_t_from, timestamp_to_sbs),
        'sourceTTo': _optional_to_sbs(query.source_t_to, timestamp_to_sbs),
        'payload': _optional_to_sbs(query.payload, event_payload_to_sbs),
        'order': {Order.DESCENDING: ('descending', None),
                  Order.ASCENDING: ('ascending', None)}[query.order],
        'orderBy': {OrderBy.TIMESTAMP: ('timestamp', None),
                    OrderBy.SOURCE_TIMESTAMP: ('sourceTimestamp', None)
                    }[query.order_by],
        'uniqueType': query.unique_type,
        'maxResults': _optional_to_sbs(query.max_results)}


def query_from_sbs(data: sbs.Data) -> QueryData:
    """Create new QueryData based on SBS data"""
    return QueryData(
        server_id=_optional_from_sbs(data['serverId']),
        event_ids=_optional_from_sbs(data['ids'], lambda ids: [
            _event_id_from_sbs(i) for i in ids]),
        event_types=_optional_from_sbs(data['types'], lambda ets: [
            tuple(et) for et in ets]),
        t_from=_optional_from_sbs(data['tFrom'], timestamp_from_sbs),
        t_to=_optional_from_sbs(data['tTo'], timestamp_from_sbs),
        source_t_from=_optional_from_sbs(data['sourceTFrom'],
                                         timestamp_from_sbs),
        source_t_to=_optional_from_sbs(data['sourceTTo'], timestamp_from_sbs),
        payload=_optional_from_sbs(data['payload'], event_payload_from_sbs),
        order={'descending': Order.DESCENDING,
               'ascending': Order.ASCENDING}[data['order'][0]],
        order_by={'timestamp': OrderBy.TIMESTAMP,
                  'sourceTimestamp': OrderBy.SOURCE_TIMESTAMP
                  }[data['orderBy'][0]],
        unique_type=data['uniqueType'],
        max_results=_optional_from_sbs(data['maxResults']))


def event_payload_to_sbs(payload: EventPayload) -> sbs.Data:
    """Convert EventPayload to SBS data"""
    if payload.type == EventPayloadType.BINARY:
        return 'binary', payload.data

    if payload.type == EventPayloadType.JSON:
        return 'json', json.encode(payload.data)

    if payload.type == EventPayloadType.SBS:
        return 'sbs', _sbs_data_to_sbs(payload.data)

    raise ValueError('unsupported payload type')


def event_payload_from_sbs(data: sbs.Data) -> EventPayload:
    """Create new EventPayload based on SBS data"""
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


def syncer_req_to_sbs(syncer_req: SyncerReq) -> sbs.Data:
    """Convert SyncerReq to SBS data"""
    return {'lastEventId': _event_id_to_sbs(syncer_req.last_event_id),
            'clientName': syncer_req.client_name}


def syncer_req_from_sbs(data: sbs.Data) -> SyncerReq:
    """Create new SyncerReq based on SBS data"""
    return SyncerReq(last_event_id=_event_id_from_sbs(data['lastEventId']),
                     client_name=data['clientName'])


def _event_id_to_sbs(event_id):
    return {'server': event_id.server,
            'session': event_id.session,
            'instance': event_id.instance}


def _event_id_from_sbs(data):
    return EventId(server=data['server'],
                   session=data['session'],
                   instance=data['instance'])


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
