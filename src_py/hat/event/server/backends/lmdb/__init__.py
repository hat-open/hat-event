"""LMDB backend"""

from hat.event.server.backends.lmdb import backend
from hat.event.server.backends.lmdb import common


info = common.BackendInfo(create=backend.create,
                          json_schema_id='hat-event://backends/lmdb.yaml#',
                          json_schema_repo=common.json_schema_repo)
