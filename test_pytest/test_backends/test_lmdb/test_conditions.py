import pytest

from hat.event.backends.lmdb import common
import hat.event.backends.lmdb.conditions


def create_event(event_type, payload):
    return common.Event(id=common.EventId(1, 2, 3),
                        type=event_type,
                        timestamp=common.now(),
                        source_timestamp=None,
                        payload=payload)


@pytest.mark.parametrize('conf, matching, not_matching', [
    ([{'subscriptions': [('a',)],
       'condition': {'type': 'json'}}],
     [create_event(('a',), common.EventPayloadJson(None)),
      create_event(('b',), None)],
     [create_event(('a',), None),
      create_event(('a',), common.EventPayloadBinary('', b''))]),

    ([{'subscriptions': [('*',)],
       'condition': {'type': 'any',
                     'conditions': [{'type': 'json',
                                     'data_value': 1},
                                    {'type': 'json',
                                     'data_value': 2}]}}],
     [create_event(('a',), common.EventPayloadJson(1)),
      create_event(('a',), common.EventPayloadJson(2))],
     [create_event(('a',), common.EventPayloadJson(3))]),

    ([{'subscriptions': [('*',)],
       'condition': {'type': 'all',
                     'conditions': [{'type': 'json',
                                     'data_path': 'x',
                                     'data_value': 1},
                                    {'type': 'json',
                                     'data_path': 'y',
                                     'data_value': 2}]}}],
     [create_event(('a',), common.EventPayloadJson({'x': 1, 'y': 2}))],
     [create_event(('a',), common.EventPayloadJson({'x': 1})),
      create_event(('a',), common.EventPayloadJson({'y': 2}))]),

    ([{'subscriptions': [('*',)],
       'condition': {'type': 'json',
                     'data_type': 'null'}}],
     [create_event(('a',), common.EventPayloadJson(None))],
     [create_event(('a',), common.EventPayloadJson(1))]),

    ([{'subscriptions': [('*',)],
       'condition': {'type': 'json',
                     'data_type': 'boolean'}}],
     [create_event(('a',), common.EventPayloadJson(True))],
     [create_event(('a',), common.EventPayloadJson(1))]),

    ([{'subscriptions': [('*',)],
       'condition': {'type': 'json',
                     'data_type': 'string'}}],
     [create_event(('a',), common.EventPayloadJson('abc'))],
     [create_event(('a',), common.EventPayloadJson(None))]),

    ([{'subscriptions': [('*',)],
       'condition': {'type': 'json',
                     'data_type': 'number'}}],
     [create_event(('a',), common.EventPayloadJson(123))],
     [create_event(('a',), common.EventPayloadJson(True))]),

    ([{'subscriptions': [('*',)],
       'condition': {'type': 'json',
                     'data_type': 'array'}}],
     [create_event(('a',), common.EventPayloadJson([1, 2, 3]))],
     [create_event(('a',), common.EventPayloadJson(None))]),

    ([{'subscriptions': [('*',)],
       'condition': {'type': 'json',
                     'data_type': 'object'}}],
     [create_event(('a',), common.EventPayloadJson({}))],
     [create_event(('a',), common.EventPayloadJson(None))]),
])
def test_conditions(conf, matching, not_matching):
    conditions = hat.event.backends.lmdb.conditions.Conditions(conf)

    for event in matching:
        result = conditions.matches(event)
        assert result is True

    for event in not_matching:
        result = conditions.matches(event)
        assert result is False
