from pathlib import Path
import abc
import enum
import platform
import typing

import lmdb

from hat import json
from hat.event.server.common import Event, EventId, EventType, Timestamp
from hat.event.server.common import *  # NOQA


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


ExtFlushCb = typing.Callable[[lmdb.Transaction], typing.Iterable[Event]]


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
