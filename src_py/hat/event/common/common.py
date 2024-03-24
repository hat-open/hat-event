from collections.abc import Collection
import enum
import importlib.resources
import typing

from hat import json
from hat import sbs
from hat import util


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


class Timestamp(typing.NamedTuple):
    s: int
    """seconds since 1970-01-01 (can be negative)"""
    us: int
    """microseconds added to timestamp seconds in range [0, 1e6)"""

    def add(self, s: float) -> 'Timestamp':
        """Create new timestamp by adding seconds to existing timestamp"""
        us = self.us + round((s - int(s)) * 1e6)
        s = self.s + int(s)
        return Timestamp(s=s + us // int(1e6),
                         us=us % int(1e6))


min_timestamp: Timestamp = Timestamp(s=-(1 << 63), us=0)
"""Minimal byte serializable timestamp value"""

max_timestamp: Timestamp = Timestamp(s=(1 << 63) - 1, us=999_999)
"""Maximal byte serializable timestamp value"""


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
