"""Common event server structures and functionality"""

from hat.event.common import *  # NOQA

from collections.abc import Iterable
import abc
import enum
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
                       events: list[RegisterEvent]
                       ) -> list[Event] | None:
        """Register events"""

    @abc.abstractmethod
    async def query(self,
                    params: QueryParams
                    ) -> QueryResult:
        """Query events"""


class BackendClosedError(Exception):
    """Backend closed"""


BackendConf: typing.TypeAlias = json.Data
"""Backend configuration"""

BackendRegisteredEventsCb: typing.TypeAlias = aio.AsyncCallable[[list[Event]],
                                                                None]
"""Backend registered events callback"""

BackendFlushedEventsCb: typing.TypeAlias = aio.AsyncCallable[[list[Event]],
                                                             None]
"""Backend flushed events callback"""

CreateBackend: typing.TypeAlias = aio.AsyncCallable[
    [BackendConf,
     BackendRegisteredEventsCb | None,
     BackendFlushedEventsCb | None],
    'Backend']
"""Create backend callable"""


class Backend(aio.Resource):
    """Backend ABC

    Backend is implemented as python module which is dynamically imported.
    It is expected that this module implements:

    * json_schema_id (str | None): JSON schema id
    * json_schema_repo (json.SchemaRepository | None): JSON schema repo
    * create (CreateBackend): create new backend instance

    If module defines JSON schema repository and JSON schema id, JSON schema
    repository will be used for additional validation of backend configuration
    with JSON schema id.

    """

    @abc.abstractmethod
    async def get_last_event_id(self,
                                server_id: int
                                ) -> EventId:
        """Get last registered event id associated with server id"""

    @abc.abstractmethod
    async def register(self,
                       events: list[Event]
                       ) -> list[Event] | None:
        """Register events"""

    @abc.abstractmethod
    async def query(self,
                    params: QueryParams
                    ) -> QueryResult:
        """Query events"""

    @abc.abstractmethod
    async def flush(self):
        """Flush internal buffers and permanently persist events"""


ModuleConf: typing.TypeAlias = json.Data
"""Module configuration"""

CreateModule: typing.TypeAlias = aio.AsyncCallable[[ModuleConf,
                                                    Engine,
                                                    Source],
                                                   'Module']
"""Create module callable"""


class Module(aio.Resource):
    """Module ABC

    Module is implemented as python module which is dynamically imported.
    It is expected that this module implements:

        * json_schema_id (str | None): JSON schema id
        * json_schema_repo (json.SchemaRepository | None): JSON schema repo
        * create (CreateModule): create new module instance

    If module defines JSON schema repository and JSON schema id, JSON schema
    repository will be used for additional validation of module configuration
    with JSON schema id.

    Module's `subscription` is constant during module's lifetime.

    Methods `on_session_start`, `on_session_stop` and `process` can be
    coroutines or regular functions.

    """

    @property
    @abc.abstractmethod
    def subscription(self) -> Subscription:
        """Subscribed event types filter"""

    async def on_session_start(self, session_id: int):
        """Called on start of a session, identified by session_id."""

    async def on_session_stop(self, session_id: int):
        """Called on stop of a session, identified by session_id."""

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

        """
