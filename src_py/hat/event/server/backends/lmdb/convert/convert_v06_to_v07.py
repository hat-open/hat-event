from pathlib import Path
import itertools
import sys

from hat.event.server.backends.lmdb.convert import v06
from hat.event.server.backends.lmdb.convert import v07


def main():
    if len(sys.argv) != 3:
        print("Usage: %s SRC_DB_PATH DST_DB_PATH", file=sys.stderr)
        sys.exit(1)

    src_path = Path(sys.argv[1])
    dst_path = Path(sys.argv[2])

    convert(src_path, dst_path)


def convert(src_path: Path,
            dst_path: Path):
    with v06.create_env(src_path) as src_env:
        src_system_db = src_env.open_db(b'system')
        server_id = _get_server_id(src_env, src_system_db)

        with v07.create_env(dst_path) as dst_env:
            dst_system_db = v07.open_db(dst_env, v07.DbType.SYSTEM)
            dst_ref_db = v07.open_db(dst_env, v07.DbType.REF)

            _convert_latest(src_env=src_env,
                            dst_env=dst_env,
                            dst_system_db=dst_system_db,
                            dst_ref_db=dst_ref_db,
                            server_id=server_id)

            _convert_ordered(src_env=src_env,
                             dst_env=dst_env,
                             dst_system_db=dst_system_db,
                             dst_ref_db=dst_ref_db,
                             server_id=server_id)


def _convert_latest(src_env, dst_env, dst_system_db, dst_ref_db, server_id):
    src_latest_db = src_env.open_db(b'latest')

    dst_latest_data_db = v07.open_db(dst_env, v07.DbType.LATEST_DATA)
    dst_latest_type_db = v07.open_db(dst_env, v07.DbType.LATEST_TYPE)

    latest_event = None
    next_event_type_ref = itertools.count(1)

    with src_env.begin(db=src_latest_db, buffers=True) as src_txn:
        for _, src_encoded_value in src_txn.cursor():
            src_event = v06.decode_event(src_encoded_value)
            dst_event = _convert_event(src_event)

            if not latest_event or latest_event.event_id < dst_event.event_id:
                latest_event = dst_event

            event_type_ref = next(next_event_type_ref)

            with dst_env.begin(db=dst_latest_data_db, write=True) as dst_txn:
                dst_txn.put(
                    v07.encode_latest_data_db_key(
                        event_type_ref),
                    v07.encode_latest_data_db_value(
                        dst_event))

            with dst_env.begin(db=dst_latest_type_db, write=True) as dst_txn:
                dst_txn.put(
                    v07.encode_latest_type_db_key(
                        event_type_ref),
                    v07.encode_latest_type_db_value(
                        dst_event.event_type))

            _update_ref_db(dst_env=dst_env,
                           dst_ref_db=dst_ref_db,
                           dst_event=dst_event,
                           event_ref=v07.LatestEventRef(event_type_ref))

    if latest_event:
        _update_system_db(dst_env=dst_env,
                          dst_system_db=dst_system_db,
                          server_id=server_id,
                          latest_event=latest_event)


def _convert_ordered(src_env, dst_env, dst_system_db, dst_ref_db, server_id):
    src_ordered_data_db = src_env.open_db(b'ordered_data')
    src_ordered_partition_db = src_env.open_db(b'ordered_partition')
    src_ordered_count_db = src_env.open_db(b'ordered_count')

    dst_ordered_data_db = v07.open_db(dst_env, v07.DbType.ORDERED_DATA)
    dst_ordered_partition_db = v07.open_db(
        dst_env, v07.DbType.ORDERED_PARTITION)
    dst_ordered_count_db = v07.open_db(dst_env, v07.DbType.ORDERED_COUNT)

    latest_event = None

    with src_env.begin(db=src_ordered_data_db, buffers=True) as src_txn:
        for src_encoded_key, src_encoded_value in src_txn.cursor():
            partition_id, src_timestamp, _ = v06.decode_uint_timestamp_uint(
                src_encoded_key)
            src_event = v06.decode_event(src_encoded_value)
            dst_timestamp = _convert_timestamp(src_timestamp)
            dst_event = _convert_event(src_event)

            if not latest_event or latest_event.event_id < dst_event.event_id:
                latest_event = dst_event

            dst_key = partition_id, dst_timestamp, dst_event.event_id

            with dst_env.begin(db=dst_ordered_data_db, write=True) as dst_txn:
                dst_txn.put(
                    v07.encode_ordered_data_db_key(
                        dst_key),
                    v07.encode_ordered_data_db_value(
                        dst_event))

            _update_ref_db(dst_env=dst_env,
                           dst_ref_db=dst_ref_db,
                           dst_event=dst_event,
                           event_ref=v07.OrderedEventRef(dst_key))

    if latest_event:
        _update_system_db(dst_env=dst_env,
                          dst_system_db=dst_system_db,
                          server_id=server_id,
                          latest_event=latest_event)

    with src_env.begin(db=src_ordered_partition_db, buffers=True) as src_txn:
        for src_encoded_key, src_encoded_value in src_txn.cursor():
            partition_id = v06.decode_uint(src_encoded_key)
            partition_data = v06.decode_json(src_encoded_value)

            with dst_env.begin(db=dst_ordered_partition_db,
                               write=True) as dst_txn:
                dst_txn.put(
                    v07.encode_ordered_partition_db_key(
                        partition_id),
                    v07.encode_ordered_partition_db_value(
                        partition_data))

    with src_env.begin(db=src_ordered_count_db, buffers=True) as src_txn:
        for src_encoded_key, src_encoded_value in src_txn.cursor():
            partition_id = v06.decode_uint(src_encoded_key)
            count = v06.decode_json(src_encoded_value)

            with dst_env.begin(db=dst_ordered_count_db,
                               write=True) as dst_txn:
                dst_txn.put(
                    v07.encode_ordered_count_db_key(
                        partition_id),
                    v07.encode_ordered_count_db_value(
                        count))


def _update_ref_db(dst_env, dst_ref_db, dst_event, event_ref):
    with dst_env.begin(db=dst_ref_db,
                       write=True,
                       buffers=True) as dst_txn:
        dst_encoded_key = v07.encode_ref_db_key(dst_event.event_id)
        dst_encoded_value = dst_txn.get(dst_encoded_key)

        event_refs = (v07.decode_ref_db_value(dst_encoded_value)
                      if dst_encoded_value else set())
        event_refs.add(event_ref)
        dst_encoded_value = v07.encode_ref_db_value(event_refs)

        dst_txn.put(dst_encoded_key, dst_encoded_value)


def _update_system_db(dst_env, dst_system_db, server_id, latest_event):
    with dst_env.begin(db=dst_system_db,
                       write=True,
                       buffers=True) as dst_txn:
        dst_encoded_key = v07.encode_system_db_key(server_id)
        dst_encoded_value = dst_txn.get(dst_encoded_key)

        event_id, _ = (v07.decode_system_db_value(dst_encoded_value)
                       if dst_encoded_value
                       else (v07.EventId(server_id, 0, 0), None))

        if latest_event.event_id > event_id:
            dst_txn.put(dst_encoded_key,
                        v07.encode_system_db_value((latest_event.event_id,
                                                    latest_event.timestamp)))


def _get_server_id(src_env, src_system_db):
    with src_env.begin(db=src_system_db, buffers=True) as txn:
        src_encoded_server_id = txn.get(b'server_id')
        return v06.decode_uint(src_encoded_server_id)


def _convert_event(src_event):
    return v07.Event(
        event_id=_convert_event_id(src_event.event_id),
        event_type=src_event.event_type,
        timestamp=_convert_timestamp(src_event.timestamp),
        source_timestamp=(_convert_timestamp(src_event.source_timestamp)
                          if src_event.source_timestamp else None),
        payload=(_convert_event_payload(src_event.payload)
                 if src_event.payload else None))


def _convert_event_id(src_event_id):
    return v07.EventId(server=src_event_id.server,
                       session=src_event_id.instance,
                       instance=src_event_id.instance)


def _convert_event_payload(src_event_payload):
    return v07.EventPayload(
        type=v07.EventPayloadType[src_event_payload.type.name],
        data=src_event_payload.data)


def _convert_timestamp(src_timestamp):
    return v07.Timestamp(s=src_timestamp.s,
                         us=src_timestamp.us)


if __name__ == '__main__':
    main()
