import pytest

from hat.event.backends.lmdb import common


server_ids = [0, 1, 1234, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF]

event_ids = [common.EventId(1, 2, 3),
             common.EventId(1, 2, 0xFFFF_FFFF),
             common.EventId(0xFFFF_FFFF, 0xFFFF_FFFF, 0x7FFF_FFFF_FFFF_FFFF)]

event_type_refs = [0, 1, 1234, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF]

partition_ids = [0, 1, 1234, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF]

timestamps = [common.now()]

source_timestamps = [None, common.now()]

timeseries_keys = [(partition_id, timestamp, event_id)
                   for partition_id in partition_ids
                   for timestamp in timestamps
                   for event_id in event_ids]

json_data = [None, 1234, "abc", {'a': [1, [], True, {}]}]

event_refs = [*(common.LatestEventRef(event_type_ref)
                for event_type_ref in event_type_refs),
              *(common.TimeseriesEventRef(timeseries_key)
                for timeseries_key in timeseries_keys)]

event_types = [(), ('a', ), ('x', 'y', 'z')]

event_payloads = [common.EventPayloadBinary('abc', b'xyz'),
                  *(common.EventPayloadJson(data)
                    for data in json_data)]

events = [common.Event(id=event_id,
                       type=event_type,
                       timestamp=timestamp,
                       source_timestamp=source_timestamp,
                       payload=payload)
          for event_id in event_ids
          for event_type in event_types
          for timestamp in timestamps
          for source_timestamp in source_timestamps
          for payload in event_payloads]

counts = [0, 1, 1234, 0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF]


@pytest.mark.parametrize('db_type, key', [
    *((common.DbType.SYSTEM_SETTINGS, settings_id)
      for settings_id in common.SettingsId),

    *((common.DbType.SYSTEM_LAST_EVENT_ID, server_id)
      for server_id in server_ids),

    *((common.DbType.SYSTEM_LAST_TIMESTAMP, server_id)
      for server_id in server_ids),

    *((common.DbType.REF, event_id)
      for event_id in event_ids),

    *((common.DbType.LATEST_DATA, event_type_ref)
      for event_type_ref in event_type_refs),

    *((common.DbType.LATEST_TYPE, event_type_ref)
      for event_type_ref in event_type_refs),

    *((common.DbType.TIMESERIES_DATA, timeseries_key)
      for timeseries_key in timeseries_keys),

    *((common.DbType.TIMESERIES_PARTITION, partition_id)
      for partition_id in partition_ids),

    *((common.DbType.TIMESERIES_COUNT, partition_id)
      for partition_id in partition_ids)
])
def test_encode_decode_key(db_type, key):
    db_def = common.db_defs[db_type]
    encoded = db_def.encode_key(key)
    decoded = db_def.decode_key(encoded)
    assert key == decoded


@pytest.mark.parametrize('db_type, value', [
    *((common.DbType.SYSTEM_SETTINGS, data)
      for data in json_data),

    *((common.DbType.SYSTEM_LAST_EVENT_ID, event_id)
      for event_id in event_ids),

    *((common.DbType.SYSTEM_LAST_TIMESTAMP, timestamp)
      for timestamp in timestamps),

    *((common.DbType.REF, set(event_refs[:i]))
      for i in range(len(event_refs))),

    *((common.DbType.LATEST_DATA, event)
      for event in events),

    *((common.DbType.LATEST_TYPE, event_type)
      for event_type in event_types),

    *((common.DbType.TIMESERIES_DATA, event)
      for event in events),

    *((common.DbType.TIMESERIES_PARTITION, data)
      for data in json_data),

    *((common.DbType.TIMESERIES_COUNT, count)
      for count in counts)
])
def test_encode_decode_value(db_type, value):
    db_def = common.db_defs[db_type]
    encoded = db_def.encode_value(value)
    decoded = db_def.decode_value(encoded)
    assert value == decoded


def test_create_env(tmp_path):
    db_path = tmp_path / 'db'
    assert not db_path.exists()

    env = common.ext_create_env(db_path)
    assert db_path.exists()

    env.close()
    assert db_path.exists()


@pytest.mark.parametrize('db_type', common.DbType)
def test_open_db(db_type, tmp_path):
    db_path = tmp_path / 'db'
    env = common.ext_create_env(db_path)

    with pytest.raises(Exception):
        common.ext_open_db(env, db_type, False)

    assert common.ext_open_db(env, db_type, True)

    env.close()

    env = common.ext_create_env(db_path)
    assert common.ext_open_db(env, db_type, False)
    env.close()
