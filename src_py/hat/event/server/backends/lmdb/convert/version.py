from pathlib import Path
import contextlib
import enum
import platform
import struct

import lmdb

from hat import json


_max_size = (1024 * 1024 * 1024 * 1024
             if platform.architecture()[0] == '64bit'
             else 2 * 1024 * 1024 * 1024)

_max_dbs = 9


class Version(enum.Enum):
    v06 = '0.6'
    v07 = '0.7'
    v09 = '0.9'


def get_version(path: Path) -> Version:
    with lmdb.Environment(str(path),
                          map_size=_max_size,
                          subdir=False,
                          max_dbs=_max_dbs,
                          readonly=True) as env:
        with contextlib.suppress(Exception):
            db = env.open_db(b'SYSTEM_SETTINGS', create=False)

            with env.begin(buffers=True) as txn:
                key = _encode_uint(0)
                value = txn.get(key, db=db)

                return Version(_decode_json(value))

        with contextlib.suppress(Exception):
            for db_name in [b'SYSTEM', b'LATEST_DATA', b'LATEST_TYPE',
                            b'ORDERED_DATA', b'ORDERED_PARTITION',
                            b'ORDERED_COUNT', b'REF']:
                env.open_db(db_name, create=False)

            return Version.v07

        with contextlib.suppress(Exception):
            for db_name in [b'system', b'latest', b'ordered_data',
                            b'ordered_partition', b'ordered_count']:
                env.open_db(db_name, create=False)

            return Version.v06

    raise Exception('unsupported version')


def _encode_uint(value):
    return struct.pack(">Q", value)


def _decode_json(data_bytes):
    return json.decode(str(data_bytes, encoding='utf-8'))
