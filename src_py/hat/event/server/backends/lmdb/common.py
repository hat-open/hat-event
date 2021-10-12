import typing

import lmdb

from hat.event.server.common import Timestamp
from hat.event.server.common import *  # NOQA


ExtFlushCb = typing.Callable[[lmdb.Transaction, Timestamp], None]
