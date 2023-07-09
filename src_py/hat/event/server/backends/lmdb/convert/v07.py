from pathlib import Path
import platform

import lmdb

from hat import json

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder


EventType = common.EventType
EventId = common.EventId
EventPayloadType = common.EventPayloadType
EventPayload = common.EventPayload
Timestamp = common.Timestamp
Event = common.Event
DbType = common.DbType
ServerId = common.ServerId
EventTypeRef = common.EventTypeRef
PartitionId = common.PartitionId
LatestEventRef = common.LatestEventRef
OrderedEventRef = common.OrderedEventRef
EventRef = common.EventRef

SystemDbKey = ServerId
SystemDbValue = tuple[EventId, Timestamp]

LatestDataDbKey = EventTypeRef
LatestDataDbValue = Event

LatestTypeDbKey = EventTypeRef
LatestTypeDbValue = EventType

OrderedDataDbKey = tuple[PartitionId, Timestamp, EventId]
OrderedDataDbValue = Event

OrderedPartitionDbKey = PartitionId
OrderedPartitionDbValue = json.Data

OrderedCountDbKey = PartitionId
OrderedCountDbValue = int

RefDbKey = EventId
RefDbValue = set[EventRef]

encode_system_db_key = encoder.encode_system_db_key
encode_system_db_value = encoder.encode_system_db_value
encode_latest_data_db_key = encoder.encode_latest_data_db_key
encode_latest_data_db_value = encoder.encode_latest_data_db_value
encode_latest_type_db_key = encoder.encode_latest_type_db_key
encode_latest_type_db_value = encoder.encode_latest_type_db_value
encode_ordered_data_db_key = encoder.encode_ordered_data_db_key
encode_ordered_data_db_value = encoder.encode_ordered_data_db_value
encode_ordered_partition_db_key = encoder.encode_ordered_partition_db_key
encode_ordered_partition_db_value = encoder.encode_ordered_partition_db_value
encode_ordered_count_db_key = encoder.encode_ordered_count_db_key
encode_ordered_count_db_value = encoder.encode_ordered_count_db_value
encode_ref_db_key = encoder.encode_ref_db_key
encode_ref_db_value = encoder.encode_ref_db_value

decode_system_db_value = encoder.decode_system_db_value
decode_ref_db_value = encoder.decode_ref_db_value


open_db = common.ext_open_db


def create_env(path: Path):
    max_dbs = len(common.DbType)
    max_db_size = (512 * 1024 * 1024 * 1024
                   if platform.architecture()[0] == '64bit'
                   else 1024 * 1024 * 1024)
    return lmdb.Environment(str(path),
                            map_size=max_db_size,
                            subdir=False,
                            max_dbs=max_dbs)
