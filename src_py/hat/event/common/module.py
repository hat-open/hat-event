from collections.abc import Collection, Iterable
import abc
import enum
import importlib
import typing

from hat import aio
from hat import json

from hat.event.common.common import (Event,
                                     QueryParams,
                                     QueryResult,
                                     RegisterEvent)
from hat.event.common.subscription import Subscription


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

    @property
    @abc.abstractmethod
    def server_id(self) -> int:
        """Event server identifier"""

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

    @abc.abstractmethod
    def restart(self):
        """Schedule engine restart"""

    @abc.abstractmethod
    def reset_monitor_ready(self):
        """Schedule reseting of monitor component's ready flag"""


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


def import_module_info(py_module_str: str) -> ModuleInfo:
    """Import module info"""
    py_module = importlib.import_module(py_module_str)
    info = py_module.info

    if not isinstance(info, ModuleInfo):
        raise Exception('invalid module implementation')

    return info
