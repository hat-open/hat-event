"""Common event server structures and functionality"""

from hat.event.common import *  # NOQA

import abc
import enum
import typing

from hat import aio
from hat import json
from hat import util

from hat.event.common import (EventId,
                              Event,
                              QueryData,
                              Subscription,
                              RegisterEvent)


SourceType = enum.Enum('SourceType', [
    'SYNCER',
    'EVENTER',
    'MODULE',
    'ENGINE'])


class Source(typing.NamedTuple):
    type: SourceType
    id: int


EventsCb: typing.TypeAlias = typing.Callable[[list[Event]], None]
"""Events callback"""


class Engine(aio.Resource):
    """Engine ABC"""

    @abc.abstractmethod
    def register_events_cb(self,
                           cb: EventsCb
                           ) -> util.RegisterCallbackHandle:
        """Register events callback"""

    @abc.abstractmethod
    async def register(self,
                       source: Source,
                       events: list[RegisterEvent]
                       ) -> list[Event | None]:
        """Register events"""

    @abc.abstractmethod
    async def query(self,
                    data: QueryData
                    ) -> list[Event]:
        """Query events"""


BackendConf: typing.TypeAlias = json.Data
"""Backend configuration"""

CreateBackend: typing.TypeAlias = aio.AsyncCallable[[BackendConf], 'Backend']
"""Create backend callable"""


class Backend(aio.Resource):
    """Backend ABC

    Backend is implemented as python module which is dynamically imported.
    It is expected that this module implements:

    * json_schema_id (typing.Optional[str]): JSON schema id
    * json_schema_repo (typing.Optional[json.SchemaRepository]):
        JSON schema repo
    * create (CreateBackend): create new backend instance

    If module defines JSON schema repository and JSON schema id, JSON schema
    repository will be used for additional validation of backend configuration
    with JSON schema id.

    """

    @abc.abstractmethod
    def register_registered_events_cb(self,
                                      cb: typing.Callable[[typing.List[Event]],
                                                          None]
                                      ) -> util.RegisterCallbackHandle:
        """Register registered events callback"""

    @abc.abstractmethod
    def register_flushed_events_cb(self,
                                   cb: typing.Callable[[typing.List[Event]],
                                                       None]
                                   ) -> util.RegisterCallbackHandle:
        """Register flushed events callback"""

    @abc.abstractmethod
    async def get_last_event_id(self,
                                server_id: int
                                ) -> EventId:
        """Get last registered event id associated with server id"""

    @abc.abstractmethod
    async def register(self,
                       events: typing.List[Event]
                       ) -> typing.List[typing.Optional[Event]]:
        """Register events"""

    @abc.abstractmethod
    async def query(self,
                    data: QueryData
                    ) -> typing.List[Event]:
        """Query events"""

    @abc.abstractmethod
    async def query_flushed(self,
                            event_id: EventId
                            ) -> typing.AsyncIterable[typing.List[Event]]:
        """Get events with the same event_id.server, and event_id.instance
        greater than provided. Iterates over lists of Events from the
        same session. Only permanently persisted events (flushed) are
        returned."""

    @abc.abstractmethod
    async def flush(self):
        """Flush internal buffers and permanently persist events"""


ModuleConf: typing.TypeAlias = json.Data

CreateModule: typing.TypeAlias = aio.AsyncCallable[[ModuleConf, Engine,
                                                    Source],
                                                   'Module']


class Module(aio.Resource):
    """Module ABC

    Module is implemented as python module which is dynamically imported.
    It is expected that this module implements:

        * json_schema_id (typing.Optional[str]): JSON schema id
        * json_schema_repo (typing.Optional[json.SchemaRepository]):
            JSON schema repo
        * create (CreateModule): create new module instance

    If module defines JSON schema repository and JSON schema id, JSON schema
    repository will be used for additional validation of module configuration
    with JSON schema id.

    Module's `subscription` is constant during module's lifetime.

    """

    @property
    @abc.abstractmethod
    def subscription(self) -> Subscription:
        """Subscribed event types filter"""

    async def on_session_start(self,
                               session_id: int):
        """Called on start of a session, identified by session_id."""

    async def on_session_stop(self,
                              session_id: int):
        """Called on stop of a session, identified by session_id."""

    @abc.abstractmethod
    async def process(self,
                      source: Source,
                      event: Event
                      ) -> typing.AsyncIterable[RegisterEvent]:
        """Process new session event.

        Provided event is matched by modules subscription filter.

        Processing of session event can result in registration of
        new register events.

        Single module session process is always called sequentially.

        """
