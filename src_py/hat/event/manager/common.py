from hat.event.common import *  # NOQA

from hat import json

from hat.event.common import (Event,
                              QueryResult,
                              EventPayloadBinary,
                              EventPayloadJson)


def event_to_json(event: Event) -> json.Data:
    return {
        'id': _event_id_to_json(event.id),
        'type': list(event.type),
        'timestamp': _timestamp_to_json(event.timestamp),
        'source_timestamp': (_timestamp_to_json(event.source_timestamp)
                             if event.source_timestamp else None),
        'payload': (_event_payload_to_json(event.payload)
                    if event.payload else None)}


def query_result_to_json(result: QueryResult) -> json.Data:
    return {'events': [event_to_json(event) for event in result.events],
            'more_follows': result.more_follows}


def _event_id_to_json(event_id):
    return {'server': event_id.server,
            'session': event_id.session,
            'instance': event_id.instance}


def _timestamp_to_json(timestamp):
    return {'s': timestamp.s,
            'us': timestamp.us}


def _event_payload_to_json(payload):
    if isinstance(payload, EventPayloadBinary):
        return {'data_type': 'binary',
                'binary_type': payload.type,
                'data': bytes(payload.data).hex()}

    if isinstance(payload, EventPayloadJson):
        return {'data_type': 'json',
                'data': payload.data}

    raise ValueError('unuspported payload type')
