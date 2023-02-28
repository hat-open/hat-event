from hat.event.common import *  # NOQA

import typing

from hat.event.common import EventId, EventType, Event


class PingMsg(typing.NamedTuple):
    pass


class PongMsg(typing.NamedTuple):
    pass


class InitMsg(typing.NamedTuple):
    client_id: str
    client_token: typing.Optional[str]
    last_event_id: typing.Optional[EventId]
    subscriptions: typing.List[EventType]


class EventsMsg(typing.NamedTuple):
    events: typing.List[Event]


Msg = typing.Union[PingMsg, PongMsg, InitMsg, EventsMsg]
