from hat.event.server.common import *  # NOQA

from pathlib import Path
import abc
import enum
import platform
import typing

import lmdb

from hat import json

from hat.event.server.common import Event, EventId, EventType, Timestamp


default_max_size = (512 * 1024 * 1024 * 1024
                    if platform.architecture()[0] == '64bit'
                    else 1024 * 1024 * 1024)


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

SystemDbKey: typing.TypeAlias = ServerId
SystemDbValue: typing.TypeAlias = tuple[EventId, Timestamp]

LatestDataDbKey: typing.TypeAlias = EventTypeRef
LatestDataDbValue: typing.TypeAlias = Event

LatestTypeDbKey: typing.TypeAlias = EventTypeRef
LatestTypeDbValue: typing.TypeAlias = EventType

OrderedDataDbKey: typing.TypeAlias = tuple[PartitionId, Timestamp, EventId]
OrderedDataDbValue: typing.TypeAlias = Event

OrderedPartitionDbKey: typing.TypeAlias = PartitionId
OrderedPartitionDbValue: typing.TypeAlias = json.Data

OrderedCountDbKey: typing.TypeAlias = PartitionId
OrderedCountDbValue: typing.TypeAlias = int

RefDbKey: typing.TypeAlias = EventId
RefDbValue: typing.TypeAlias = typing.Set['EventRef']


class LatestEventRef(typing.NamedTuple):
    key: LatestDataDbKey


class OrderedEventRef(typing.NamedTuple):
    key: OrderedDataDbKey


EventRef: typing.TypeAlias = LatestEventRef | OrderedEventRef


class EventRefChange(typing.NamedTuple):
    event_id: EventId
    added: set[EventRef]
    removed: set[EventRef]


ExtFlushCb: typing.TypeAlias = typing.Callable[[lmdb.Transaction],
                                               typing.Iterable[Event]]


class Flushable(abc.ABC):

    @abc.abstractmethod
    def create_ext_flush(self) -> ExtFlushCb:
        pass


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
                db_type: DbType
                ) -> lmdb._Database:
    return env.open_db(db_type.name.encode('utf-8'))
