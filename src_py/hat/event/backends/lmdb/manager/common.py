from hat.event.backends.lmdb.common import *  # NOQA

from hat import json

from hat.event.backends.lmdb.common import (EventId,
                                            Timestamp,
                                            Event,
                                            EventPayloadJson,
                                            EventPayloadBinary)


def event_id_to_json(event_id: EventId) -> json.Data:
    return {'server': event_id.server,
            'session': event_id.session,
            'instance': event_id.instance}


def timestamp_to_json(timestamp: Timestamp) -> json.Data:
    return {'s': timestamp.s,
            'us': timestamp.us}


def event_to_json(event: Event) -> json.Data:
    return {'id': event_id_to_json(event.id),
            'type': list(event.type),
            'timestamp': timestamp_to_json(event.timestamp),
            'source_timestamp': (timestamp_to_json(event.source_timestamp)
                                 if event.source_timestamp else None),
            'payload': _event_payload_to_json(event.payload)}


def _event_payload_to_json(payload):
    if payload is None:
        return None

    if isinstance(payload, EventPayloadBinary):
        return {'type': 'BINARY',
                'subtype': payload.type,
                'data': bytes(payload.data).hex()}

    if isinstance(payload, EventPayloadJson):
        return {'type': 'JSON',
                'data': payload.data}

    raise ValueError('unsupported payload type')
