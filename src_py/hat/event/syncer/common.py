from hat.event.common import *  # NOQA

import typing

from hat import sbs

from hat.event.common import EventId, EventType


class SyncerReq(typing.NamedTuple):
    last_event_id: EventId
    client_name: str
    client_token: typing.Optional[str]
    subscriptions: typing.List[EventType]


def syncer_req_to_sbs(syncer_req: SyncerReq) -> sbs.Data:
    """Convert SyncerReq to SBS data"""
    return {'lastEventId': _event_id_to_sbs(syncer_req.last_event_id),
            'clientName': syncer_req.client_name,
            'clientToken': _optional_to_sbs(syncer_req.client_token),
            'subscriptions': [list(i) for i in syncer_req.subscriptions]}


def syncer_req_from_sbs(data: sbs.Data) -> SyncerReq:
    """Create new SyncerReq based on SBS data"""
    return SyncerReq(last_event_id=_event_id_from_sbs(data['lastEventId']),
                     client_name=data['clientName'],
                     client_token=_optional_from_sbs(data['clientToken']),
                     subscriptions=[tuple(i) for i in data['subscriptions']])


def _event_id_to_sbs(event_id):
    return {'server': event_id.server,
            'session': event_id.session,
            'instance': event_id.instance}


def _event_id_from_sbs(data):
    return EventId(server=data['server'],
                   session=data['session'],
                   instance=data['instance'])


def _optional_to_sbs(value, fn=lambda i: i):
    return ('value', fn(value)) if value is not None else ('none', None)


def _optional_from_sbs(data, fn=lambda i: i):
    return fn(data[1]) if data[0] == 'value' else None
