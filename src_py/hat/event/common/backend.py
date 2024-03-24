from collections.abc import Collection
import abc
import importlib
import typing

from hat import aio
from hat import json

from hat.event.common.common import (EventId,
                                     Event,
                                     QueryParams,
                                     QueryResult)


class BackendClosedError(Exception):
    """Backend closed"""


class Backend(aio.Resource):
    """Backend ABC"""

    @abc.abstractmethod
    async def get_last_event_id(self,
                                server_id: int
                                ) -> EventId:
        """Get last registered event id associated with server id"""

    @abc.abstractmethod
    async def register(self,
                       events: Collection[Event]
                       ) -> Collection[Event] | None:
        """Register events"""

    @abc.abstractmethod
    async def query(self,
                    params: QueryParams
                    ) -> QueryResult:
        """Query events"""

    @abc.abstractmethod
    async def flush(self):
        """Flush internal buffers and permanently persist events"""


BackendConf: typing.TypeAlias = json.Data
"""Backend configuration"""

BackendRegisteredEventsCb: typing.TypeAlias = aio.AsyncCallable[
    [Collection[Event]],
    None]
"""Backend registered events callback"""

BackendFlushedEventsCb: typing.TypeAlias = aio.AsyncCallable[
    [Collection[Event]],
    None]
"""Backend flushed events callback"""

CreateBackend: typing.TypeAlias = aio.AsyncCallable[
    [BackendConf,
     BackendRegisteredEventsCb | None,
     BackendFlushedEventsCb | None],
    Backend]
"""Create backend callable"""


class BackendInfo(typing.NamedTuple):
    """Backend info

    Backend is implemented as python module which is dynamically imported.
    It is expected that this module contains `info` which is instance of
    `BackendInfo`.

    If backend defines JSON schema repository and JSON schema id, JSON schema
    repository will be used for additional validation of backend configuration
    with JSON schema id.

    """
    create: CreateBackend
    json_schema_id: str | None = None
    json_schema_repo: json.SchemaRepository | None = None


def import_backend_info(py_module_str: str) -> BackendInfo:
    """Import backend info"""
    py_module = importlib.import_module(py_module_str)
    info = py_module.info

    if not isinstance(info, BackendInfo):
        raise Exception('invalid backend implementation')

    return info
