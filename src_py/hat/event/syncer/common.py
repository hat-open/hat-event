from hat.event.common import *  # NOQA

import typing

from hat import sbs

from hat.event.common import EventId, EventType


class SyncerInitReq(typing.NamedTuple):
    last_event_id: EventId
    client_name: str
    client_token: str | None
    subscriptions: list[EventType]


SyncerInitRes = str | None


def syncer_init_req_to_sbs(req: SyncerInitReq) -> sbs.Data:
    """Convert SyncerInitReq to SBS data"""
    return {'lastEventId': _event_id_to_sbs(req.last_event_id),
            'clientName': req.client_name,
            'clientToken': _optional_to_sbs(req.client_token),
            'subscriptions': [list(i) for i in req.subscriptions]}


def syncer_init_req_from_sbs(data: sbs.Data) -> SyncerInitReq:
    """Create new SyncerInitReq based on SBS data"""
    return SyncerInitReq(last_event_id=_event_id_from_sbs(data['lastEventId']),
                         client_name=data['clientName'],
                         client_token=_optional_from_sbs(data['clientToken']),
                         subscriptions=[tuple(i)
                                        for i in data['subscriptions']])


def syncer_init_res_to_sbs(res: SyncerInitRes) -> sbs.Data:
    """Convert SyncerInitRes to SBS data"""
    return ('success', None) if res is None else ('error', res)


def syncer_init_res_from_sbs(data: sbs.Data) -> SyncerInitRes:
    """Create new SyncerInitRes based on SBS data"""
    return data[1]


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
