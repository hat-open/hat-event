from hat.event.server.backends.lmdb.common import *  # NOQA

from hat import json

from hat.event.server.common import EventId, Timestamp, Event, EventPayloadType


def event_id_to_json(event_id: EventId) -> json.Data:
    return {'server': event_id.server,
            'session': event_id.session,
            'instance': event_id.instance}


def timestamp_to_json(timestamp: Timestamp) -> json.Data:
    return {'s': timestamp.s,
            'us': timestamp.us}


def event_to_json(event: Event) -> json.Data:
    return {'event_id': event_id_to_json(event.event_id),
            'event_type': list(event.event_type),
            'timestamp': timestamp_to_json(event.timestamp),
            'source_timestamp': (timestamp_to_json(event.source_timestamp)
                                 if event.source_timestamp else None),
            'payload': _event_payload_to_json(event.payload)}


def _event_payload_to_json(payload):
    if payload.type == EventPayloadType.BINARY:
        return {'type': 'BINARY'}

    if payload.type == EventPayloadType.SBS:
        return {'type': 'SBS'}

    if payload.type == EventPayloadType.JSON:
        return {'type': 'JSON',
                'data': payload.data}

    raise ValueError('unsupported payload type')
