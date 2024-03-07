from collections.abc import Collection
import enum
import importlib.resources
import typing

from hat import json
from hat import sbs
from hat import util

from hat.event.common.timestamp import (Timestamp,
                                        timestamp_to_sbs,
                                        timestamp_from_sbs)


with importlib.resources.as_file(importlib.resources.files(__package__) /
                                 'json_schema_repo.json') as _path:
    json_schema_repo: json.SchemaRepository = json.SchemaRepository(
        json.json_schema_repo,
        json.SchemaRepository.from_json(_path))
    """JSON schema repository"""

with importlib.resources.as_file(importlib.resources.files(__package__) /
                                 'sbs_repo.json') as _path:
    sbs_repo: sbs.Repository = sbs.Repository.from_json(_path)
    """SBS schema repository"""

ServerId: typing.TypeAlias = int
"""Server identifier"""

SessionId: typing.TypeAlias = int
"""Session identifier"""

InstanceId: typing.TypeAlias = int
"""Event instance identifier"""

EventTypeSegment: typing.TypeAlias = str
"""Event type segment"""

EventType: typing.TypeAlias = tuple[EventTypeSegment, ...]
"""Event type"""


class Status(enum.Enum):
    STANDBY = 'standby'
    OPERATIONAL = 'operational'


class Order(enum.Enum):
    DESCENDING = 'descending'
    ASCENDING = 'ascending'


class OrderBy(enum.Enum):
    TIMESTAMP = 'timestamp'
    SOURCE_TIMESTAMP = 'sourceTimestamp'


class EventId(typing.NamedTuple):
    server: ServerId
    session: SessionId
    instance: InstanceId


class EventPayloadBinary(typing.NamedTuple):
    type: str
    data: util.Bytes


class EventPayloadJson(typing.NamedTuple):
    data: json.Data


EventPayload: typing.TypeAlias = EventPayloadBinary | EventPayloadJson


class Event(typing.NamedTuple):
    """Event

    Operators `>` and `<` test for natural order where it is assumed that
    first operand is registered before second operand.

    """

    id: EventId
    type: EventType
    timestamp: Timestamp
    source_timestamp: Timestamp | None
    payload: EventPayload | None

    def __lt__(self, other):
        if not isinstance(other, Event):
            return NotImplemented

        if self.id == other.id:
            return False

        if self.id.server == other.id.server:
            return self.id < other.id

        if self.timestamp != other.timestamp:
            return self.timestamp < other.timestamp

        return True

    def __gt__(self, other):
        if not isinstance(other, Event):
            return NotImplemented

        if self.id == other.id:
            return False

        if self.id.server == other.id.server:
            return self.id > other.id

        if self.timestamp != other.timestamp:
            return self.timestamp > other.timestamp

        return False


class RegisterEvent(typing.NamedTuple):
    type: EventType
    source_timestamp: Timestamp | None
    payload: EventPayload | None


class QueryLatestParams(typing.NamedTuple):
    event_types: Collection[EventType] | None = None


class QueryTimeseriesParams(typing.NamedTuple):
    event_types: Collection[EventType] | None = None
    t_from: Timestamp | None = None
    t_to: Timestamp | None = None
    source_t_from: Timestamp | None = None
    source_t_to: Timestamp | None = None
    order: Order = Order.DESCENDING
    order_by: OrderBy = OrderBy.TIMESTAMP
    max_results: int | None = None
    last_event_id: EventId | None = None


class QueryServerParams(typing.NamedTuple):
    server_id: ServerId
    persisted: bool = False
    max_results: int | None = None
    last_event_id: EventId | None = None


QueryParams: typing.TypeAlias = (QueryLatestParams |
                                 QueryTimeseriesParams |
                                 QueryServerParams)


class QueryResult(typing.NamedTuple):
    events: Collection[Event]
    more_follows: bool


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
