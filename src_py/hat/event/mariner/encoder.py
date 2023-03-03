import base64

from hat import json

from hat.event.mariner import common


def encode_msg(msg: common.Msg) -> json.Data:
    if isinstance(msg, common.PingMsg):
        return {'type': 'ping'}

    if isinstance(msg, common.PongMsg):
        return {'type': 'pong'}

    if isinstance(msg, common.InitMsg):
        last_event_id = (_encode_event_id(msg.last_event_id)
                         if msg.last_event_id else None)
        subscriptions = [_encode_event_type(i)
                         for i in msg.subscriptions]

        return {'type': 'init',
                'client_id': msg.client_id,
                'client_token': msg.client_token,
                'last_event_id': last_event_id,
                'subscriptions': subscriptions}

    if isinstance(msg, common.EventsMsg):
        events = [_encode_event(event) for event in msg.events]

        return {'type': 'events',
                'events': events}

    raise ValueError('unsupported message')


def decode_msg(msg: json.Data):
    if msg['type'] == 'ping':
        return common.PingMsg()

    if msg['type'] == 'pong':
        return common.PongMsg()

    if msg['type'] == 'init':
        client_id = msg['client_id']
        if not isinstance(client_id, str):
            raise ValueError('invalid client id')

        client_token = msg['client_token']
        if not (client_token is None or isinstance(client_token, str)):
            raise ValueError('invalid client token')

        last_event_id = (_decode_event_id(msg['last_event_id'])
                         if msg['last_event_id'] is not None else None)

        if not isinstance(msg['subscriptions'], list):
            raise ValueError('invalid subscriptions')
        subscriptions = [_decode_event_type(i) for i in msg['subscriptions']]

        return common.InitMsg(client_id=client_id,
                              client_token=client_token,
                              last_event_id=last_event_id,
                              subscriptions=subscriptions)

    if msg['type'] == 'events':
        if not isinstance(msg['events'], list):
            raise ValueError('invalid events')
        events = [_decode_event(i) for i in msg['events']]

        return common.EventsMsg(events=events)

    raise ValueError('invalid message type')


def _encode_event_id(event_id):
    return {'server': event_id.server,
            'session': event_id.session,
            'instance': event_id.instance}


def _decode_event_id(event_id):
    server_id = event_id['server']
    if not isinstance(server_id, int):
        raise ValueError('invalid server id')

    session_id = event_id['session']
    if not isinstance(session_id, int):
        raise ValueError('invalid session id')

    instance_id = event_id['instance']
    if not isinstance(instance_id, int):
        raise ValueError('invalid instance id')

    return common.EventId(server=server_id,
                          session=session_id,
                          instance=instance_id)


def _encode_event_type(event_type):
    return list(event_type)


def _decode_event_type(event_type):
    if not isinstance(event_type, list):
        raise ValueError('invalid event type')

    for i in event_type:
        if not isinstance(i, str):
            raise ValueError('invalid event type')

    return tuple(event_type)


def _encode_timestamp(timestamp):
    return {'s': timestamp.s,
            'us': timestamp.us}


def _decode_timestamp(timestamp):
    s = timestamp['s']
    if not (isinstance(s, int)):
        raise ValueError('invalid seconds')

    us = timestamp['us']
    if not (isinstance(us, int) and 0 <= us < 1_000_000):
        raise ValueError('invalid microseconds')

    return common.Timestamp(s=s,
                            us=us)


def _encode_event_payload(payload):
    if payload.type == common.EventPayloadType.BINARY:
        data = str(base64.b64encode(payload.data), 'utf-8')

        return {'type': 'binary',
                'data': data}

    if payload.type == common.EventPayloadType.JSON:
        return {'type': 'json',
                'data': payload.data}

    if payload.type == common.EventPayloadType.SBS:
        sbs_data = str(base64.b64encode(payload.data.data), 'utf-8')

        return {'type': 'sbs',
                'data': {'module': payload.data.module,
                         'type': payload.data.type,
                         'data': sbs_data}}

    raise ValueError('unsupported payload type')


def _decode_event_payload(payload):
    if payload['type'] == 'binary':
        data = base64.b64decode(bytes(payload['data'], 'utf-8'))

        return common.EventPayload(
            type=common.EventPayloadType.BINARY,
            data=data)

    if payload['type'] == 'json':
        return common.EventPayload(
            type=common.EventPayloadType.JSON,
            data=payload['data'])

    if payload['type'] == 'sbs':
        sbs_module = payload['data']['module']
        if not (sbs_module is None or isinstance(sbs_module, str)):
            raise ValueError('invalid sbs module')

        sbs_type = payload['data']['type']
        if not isinstance(sbs_type, str):
            raise ValueError('invalid sbs type')

        sbs_data = base64.b64decode(bytes(payload['data']['data'], 'utf-8'))

        return common.EventPayload(
            type=common.EventPayloadType.SBS,
            data=common.SbsData(module=sbs_module,
                                type=sbs_type,
                                data=sbs_data))

    raise ValueError('invalid payload type')


def _encode_event(event):
    event_id = _encode_event_id(event.event_id)
    event_type = _encode_event_type(event.event_type)
    timestamp = _encode_timestamp(event.timestamp)
    source_timestamp = (_encode_timestamp(event.source_timestamp)
                        if event.source_timestamp else None)
    payload = (_encode_event_payload(event.payload)
               if event.payload else None)

    return {'id': event_id,
            'type': event_type,
            'timestamp': timestamp,
            'source_timestamp': source_timestamp,
            'payload': payload}


def _decode_event(event):
    event_id = _decode_event_id(event['id'])
    event_type = _decode_event_type(event['type'])
    timestamp = _decode_timestamp(event['timestamp'])
    source_timestamp = (_decode_timestamp(event['source_timestamp'])
                        if event['source_timestamp'] is not None else None)
    payload = (_decode_event_payload(event['payload'])
               if event['payload'] is not None else None)

    return common.Event(event_id=event_id,
                        event_type=event_type,
                        timestamp=timestamp,
                        source_timestamp=source_timestamp,
                        payload=payload)
