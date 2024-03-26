"""LMDB backend"""

from hat.event.backends.lmdb import common
from hat.event.backends.lmdb.backend import create


info = common.BackendInfo(create=create,
                          json_schema_id='hat-event://backends/lmdb.yaml',
                          json_schema_repo=common.json_schema_repo)
