from pathlib import Path

from hat.event.backends.lmdb.convert import v07
from hat.event.backends.lmdb.convert import v09


def convert(src_path: Path,
            dst_path: Path):
    with v07.create_env(src_path) as src_env:
        with v09.create_env(dst_path) as dst_env:
            src_dbs = {db_type: v07.open_db(src_env, db_type)
                       for db_type in v07.DbType}
            dst_dbs = {db_type: v09.open_db(dst_env, db_type)
                       for db_type in v09.DbType}

            with src_env.begin(buffers=True) as src_txn:
                _convert_system_db(src_txn, src_dbs, dst_env, dst_dbs)
                _convert_ref_db(src_txn, src_dbs, dst_env, dst_dbs)
                _convert_latest_db(src_txn, src_dbs, dst_env, dst_dbs)
                _convert_timeseries_db(src_txn, src_dbs, dst_env, dst_dbs)


def _convert_system_db(src_txn, src_dbs, dst_env, dst_dbs):
    v09.write(dst_env, dst_dbs, v09.DbType.SYSTEM_SETTINGS,
              v09.SettingsId.VERSION, v09.version)

    with src_txn.cursor(db=src_dbs[v07.SYSTEM]) as src_cursor:
        for src_key, src_value in src_cursor:
            server_id = v07.decode_system_db_key(src_key)
            src_event_id, src_timestamp = v07.decode_system_db_value(src_value)

            dst_event_id = _convert_event_id(src_event_id)
            dst_timestamp = _convert_timestamp(src_timestamp)

            v09.write(dst_env, dst_dbs, v09.DbType.SYSTEM_LAST_EVENT_ID,
                      server_id, dst_event_id)

            v09.write(dst_env, dst_dbs, v09.DbType.SYSTEM_LAST_TIMESTAMP,
                      server_id, dst_timestamp)


def _convert_ref_db(src_txn, src_dbs, dst_env, dst_dbs):
    with src_txn.cursor(db=src_dbs[v07.REF]) as src_cursor:
        for src_key, src_value in src_cursor:
            src_event_id = v07.decode_ref_db_key(src_key)
            src_event_refs = v07.decode_ref_db_value(src_value)

            dst_event_id = _convert_event_id(src_event_id)
            dst_event_refs = {_convert_event_ref(src_event_ref)
                              for src_event_ref in src_event_refs}

            v09.write(dst_env, dst_dbs, v09.DbType.REF,
                      dst_event_id, dst_event_refs)


def _convert_latest_db(src_txn, src_dbs, dst_env, dst_dbs):
    with src_txn.cursor(db=src_dbs[v07.LATEST_DATA]) as src_cursor:
        for src_key, src_value in src_cursor:
            event_type_ref = v07.decode_latest_data_db_key(src_key)
            src_event = v07.decode_latest_data_db_value(src_value)

            dst_event = _convert_event(src_event)

            v09.write(dst_env, dst_dbs, v09.DbType.LATEST_DATA,
                      event_type_ref, dst_event)

    with src_txn.cursor(db=src_dbs[v07.LATEST_TYPE]) as src_cursor:
        for src_key, src_value in src_cursor:
            event_type_ref = v07.decode_latest_type_db_key(src_key)
            event_type = v07.decode_latest_type_db_value(src_value)

            v09.write(dst_env, dst_dbs, v09.DbType.LATEST_TYPE,
                      event_type_ref, event_type)


def _convert_timeseries_db(src_txn, src_dbs, dst_env, dst_dbs):
    with src_txn.cursor(db=src_dbs[v07.ORDERED_DATA]) as src_cursor:
        for src_key, src_value in src_cursor:
            partition_id, src_timestamp, src_event_id = \
                v07.decode_ordered_data_db_key(src_key)
            src_event = v07.decode_ordered_data_db_value(src_value)

            dst_timestamp = _convert_timestamp(src_timestamp)
            dst_event_id = _convert_event_id(src_event_id)
            dst_event = _convert_event(src_event)

            v09.write(dst_env, dst_dbs, v09.DbType.TIMESERIES_DATA,
                      (partition_id, dst_timestamp, dst_event_id), dst_event)

    with src_txn.cursor(db=src_dbs[v07.ORDERED_PARTITION]) as src_cursor:
        for src_key, src_value in src_cursor:
            partition_id = v07.decode_ordered_partition_db_key(src_key)
            src_partition_data = v07.decode_ordered_partition_db_value(
                src_value)

            dst_partition_data = _convert_partition_data(src_partition_data)

            v09.write(dst_env, dst_dbs, v09.DbType.TIMESERIES_PARTITION,
                      partition_id, dst_partition_data)

    with src_txn.cursor(db=src_dbs[v07.ORDERED_COUNT]) as src_cursor:
        for src_key, src_value in src_cursor:
            partition_id = v07.decode_ordered_count_db_key(src_key)
            count = v07.decode_ordered_count_db_value(src_value)

            v09.write(dst_env, dst_dbs, v09.DbType.TIMESERIES_COUNT,
                      partition_id, count)


def _convert_event_id(src_event_id):
    return v09.EventId(*src_event_id)


def _convert_timestamp(src_timestamp):
    return v09.Timestamp(*src_timestamp)


def _convert_event_ref(src_event_ref):
    if isinstance(src_event_ref, v07.LatestEventRef):
        return v09.LatestEventRef(src_event_ref.key)

    if isinstance(src_event_ref, v07.OrderedEventRef):
        partition_id, src_timestamp, src_event_id = src_event_ref.key

        dst_timestamp = _convert_timestamp(src_timestamp)
        dst_event_id = _convert_event_id(src_event_id)

        dst_key = partition_id, dst_timestamp, dst_event_id
        return v09.LatestEventRef(dst_key)

    raise ValueError('unsupported event reference')


def _convert_event(src_event):
    return v09.Event(
        id=_convert_event_id(src_event.event_id),
        type=src_event.event_type,
        timestamp=_convert_timestamp(src_event.timestamp),
        source_timestamp=(_convert_timestamp(src_event.source_timestamp)
                          if src_event.source_timestamp else None),
        payload=_convert_payload(src_event.payload))


def _convert_payload(src_payload):
    if src_payload is None:
        return

    if src_payload.type == v07.EventPayloadType.BINARY:
        return v09.EventPayloadBinary(type='',
                                      data=src_payload.data)

    if src_payload.type == v07.EventPayloadType.JSON:
        pass

    if src_payload.type == v07.EventPayloadType.SBS:
        binary_type = (f'{src_payload.data.module}.{src_payload.data.type}'
                       if src_payload.data.module is not None else
                       src_payload.data.type)
        return v09.EventPayloadBinary(type=binary_type,
                                      data=src_payload.data.data)

    raise ValueError('unsupported payload type')


def _convert_partition_data(src_data):
    src_order_by = v07.OrderBy(src_data['order'])
    dst_order_by = _convert_order_by(src_order_by)

    return {'order': dst_order_by.value,
            'subscriptions': src_data['subscriptions']}


def _convert_order_by(src_order_by):
    return v09.OrderBy[src_order_by.name]
