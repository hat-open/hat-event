from hat.event.common import *  # NOQA

import typing

from hat.event.common import EventId, EventType, Event


class PingMsg(typing.NamedTuple):
    pass


class PongMsg(typing.NamedTuple):
    pass


class InitMsg(typing.NamedTuple):
    client_id: str
    client_token: str | None
    last_event_id: EventId | None
    subscriptions: list[EventType]


class EventsMsg(typing.NamedTuple):
    events: list[Event]


Msg: typing.TypeAlias = PingMsg | PongMsg | InitMsg | EventsMsg
