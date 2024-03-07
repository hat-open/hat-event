"""Common event server structures and functionality"""

from hat.event.common import *  # NOQA

from collections.abc import Collection, Iterable
import abc
import enum
import importlib
import typing

from hat import aio
from hat import json

from hat.event.common import (EventId,
                              Event,
                              QueryParams,
                              QueryResult,
                              Subscription,
                              RegisterEvent)


class SourceType(enum.Enum):
    EVENTER = 1
    MODULE = 2
    ENGINE = 3
    SERVER = 4


class Source(typing.NamedTuple):
    type: SourceType
    id: int


class Engine(aio.Resource):
    """Engine ABC"""

    @abc.abstractmethod
    async def register(self,
                       source: Source,
                       events: Collection[RegisterEvent]
                       ) -> Collection[Event] | None:
        """Register events"""

    @abc.abstractmethod
    async def query(self,
                    params: QueryParams
                    ) -> QueryResult:
        """Query events"""


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


class Module(aio.Resource):
    """Module ABC"""

    @property
    @abc.abstractmethod
    def subscription(self) -> Subscription:
        """Subscribed event types filter.

        `subscription` is constant during module's lifetime.

        """

    async def on_session_start(self, session_id: int):
        """Called on start of a session, identified by session_id.

        This method can be coroutine or regular function.

        """

    async def on_session_stop(self, session_id: int):
        """Called on stop of a session, identified by session_id.

        This method can be coroutine or regular function.

        """

    @abc.abstractmethod
    async def process(self,
                      source: Source,
                      event: Event
                      ) -> Iterable[RegisterEvent] | None:
        """Process new session event.

        Provided event is matched by modules subscription filter.

        Processing of session event can result in registration of
        new register events.

        Single module session process is always called sequentially.

        This method can be coroutine or regular function.

        """


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


ModuleConf: typing.TypeAlias = json.Data
"""Module configuration"""

CreateModule: typing.TypeAlias = aio.AsyncCallable[
    [ModuleConf, Engine, Source],
    Module]
"""Create module callable"""


class ModuleInfo(typing.NamedTuple):
    """Module info

    Module is implemented as python module which is dynamically imported.
    It is expected that this module contains `info` which is instance of
    `ModuleInfo`.

    If module defines JSON schema repository and JSON schema id, JSON schema
    repository will be used for additional validation of module configuration
    with JSON schema id.

    """
    create: CreateModule
    json_schema_id: str | None = None
    json_schema_repo: json.SchemaRepository | None = None


def import_backend_info(py_module_str: str) -> BackendInfo:
    """Import backend info"""
    py_module = importlib.import_module(py_module_str)
    info = py_module.info

    if not isinstance(info, BackendInfo):
        raise Exception('invalid backend implementation')

    return info


def import_module_info(py_module_str: str) -> ModuleInfo:
    """Import module info"""
    py_module = importlib.import_module(py_module_str)
    info = py_module.info

    if not isinstance(info, ModuleInfo):
        raise Exception('invalid module implementation')

    return info
