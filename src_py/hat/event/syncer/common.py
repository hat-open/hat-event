from hat.event.common import *  # NOQA

import typing

from hat import sbs

from hat.event.common import EventId


class SyncerReq(typing.NamedTuple):
    last_event_id: EventId
    client_name: str


def syncer_req_to_sbs(syncer_req: SyncerReq) -> sbs.Data:
    """Convert SyncerReq to SBS data"""
    return {'lastEventId': _event_id_to_sbs(syncer_req.last_event_id),
            'clientName': syncer_req.client_name}


def syncer_req_from_sbs(data: sbs.Data) -> SyncerReq:
    """Create new SyncerReq based on SBS data"""
    return SyncerReq(last_event_id=_event_id_from_sbs(data['lastEventId']),
                     client_name=data['clientName'])


def _event_id_to_sbs(event_id):
    return {'server': event_id.server,
            'session': event_id.session,
            'instance': event_id.instance}


def _event_id_from_sbs(data):
    return EventId(server=data['server'],
                   session=data['session'],
                   instance=data['instance'])
