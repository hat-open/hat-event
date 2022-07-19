from pathlib import Path
import abc
import collections
import enum
import typing

import lmdb

from hat import json
from hat.event.server.common import Event, EventId, EventType, Timestamp, now
from hat.event.server.common import *  # NOQA


class DbType(enum.Enum):
    SYSTEM = 0
    LATEST_DATA = 1
    LATEST_TYPE = 2
    ORDERED_DATA = 3
    ORDERED_PARTITION = 4
    ORDERED_COUNT = 5
    REF = 6


ServerId = int
EventTypeRef = int
PartitionId = int

SystemDbKey = ServerId
SystemDbValue = typing.Tuple[EventId, Timestamp]

LatestDataDbKey = EventTypeRef
LatestDataDbValue = Event

LatestTypeDbKey = EventTypeRef
LatestTypeDbValue = EventType

OrderedDataDbKey = typing.Tuple[PartitionId, Timestamp, EventId]
OrderedDataDbValue = Event

OrderedPartitionDbKey = PartitionId
OrderedPartitionDbValue = json.Data

OrderedCountDbKey = PartitionId
OrderedCountDbValue = int

RefDbKey = EventId
RefDbValue = typing.Set['EventRef']


class LatestEventRef(typing.NamedTuple):
    key: LatestDataDbKey


class OrderedEventRef(typing.NamedTuple):
    key: OrderedDataDbKey


EventRef = typing.Union[LatestEventRef,
                        OrderedEventRef]


class EventRefChange(typing.NamedTuple):
    event_id: EventId
    added: typing.Set[EventRef]
    removed: typing.Set[EventRef]


def ext_create_env(path: Path,
                   max_size: int
                   ) -> lmdb.Environment:
    return lmdb.Environment(str(path),
                            map_size=max_size,
                            subdir=False,
                            max_dbs=len(DbType))


def ext_open_db(env: lmdb.Environment,
                db_type: DbType
                ) -> lmdb._Database:
    return env.open_db(db_type.name.encode('utf-8'))


class FlushContext:

    def __init__(self, transaction: lmdb.Transaction):
        self._transaction = transaction
        self._timestamp = now()
        self._events = {}
        self._changes = {}

    @property
    def transaction(self) -> lmdb.Transaction:
        return self._transaction

    @property
    def timestamp(self) -> Timestamp:
        return self._timestamp

    def add_event_ref(self,
                      event: Event,
                      ref: EventRef):
        self._events[event.event_id] = event

        change = self._changes.get(event.event_id)
        if not change:
            change = EventRefChange(event.event_id, set(), set())
            self._changes[event.event_id] = change

        change.added.add(ref)

    def remove_event_ref(self,
                         event_id: EventId,
                         ref: EventRef):
        change = self._changes.get(event_id)
        if not change:
            change = EventRefChange(event_id, set(), set())
            self._changes[event_id] = change

        if ref in change.added:
            change.added.remove(ref)

        else:
            change.removed.add(ref)

    def get_events(self) -> typing.Iterable[typing.List[Event]]:
        event_ids = sorted(change.event_id
                           for change in self._changes.values()
                           if change.added - change.removed)

        events = collections.deque()
        session = collections.deque()

        for event_id in event_ids:
            if session and session[0].event_id.session != event_id.session:
                events.append(list(session))
                session = collections.deque()
            session.append(self._events[event_id])

        if session:
            events.append(list(session))

        return events

    def get_changes(self) -> typing.Iterable[EventRefChange]:
        for change in self._changes.values():
            if change.added or change.removed:
                yield change


ExtFlushCb = typing.Callable[[FlushContext], None]


class Flushable(abc.ABC):

    @abc.abstractmethod
    def create_ext_flush(self) -> ExtFlushCb:
        pass
