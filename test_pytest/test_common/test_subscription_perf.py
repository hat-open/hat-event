import collections
import math

import pytest

from hat.event.common.subscription.pysubscription import PySubscription
from hat.event.common.subscription.csubscription import CSubscription


pytestmark = pytest.mark.perf


subscription_classes = [PySubscription, CSubscription]


def get_event_types(event_type_size, count):
    event_types = collections.deque()
    base = (math.ceil(math.log(count, event_type_size))
            if event_type_size > 1 else count)
    if base < 1:
        base = 1
    for i in range(count):
        event_type = collections.deque()
        rest = i
        for j in range(event_type_size - 1, -1, -1):
            digit = rest // (base ** j)
            rest = rest - digit * (base ** j)
            event_type.append(f'segment {digit}')
        event_types.append(tuple(event_type))
    return event_types


@pytest.mark.parametrize("cls", subscription_classes)
@pytest.mark.parametrize("event_type_size", [1, 3, 5, 10])
@pytest.mark.parametrize("event_type_count", [1, 100, 10000, 1000000])
def test_create(duration, cls, event_type_size, event_type_count):
    description = (f'subscription create - '
                   f'class: {cls.__name__}; '
                   f'event_type_size: {event_type_size}; '
                   f'event_type_count: {event_type_count}')

    event_types = get_event_types(event_type_size, event_type_count)

    with duration(description):
        cls(event_types)


@pytest.mark.parametrize("cls", subscription_classes)
@pytest.mark.parametrize("event_type_size", [1, 3, 5, 10])
@pytest.mark.parametrize("matches_count", [1, 100, 10000, 1000000])
def test_matches(duration, cls, event_type_size, matches_count):

    description = (f'subscription matches - '
                   f'class: {cls.__name__}; '
                   f'event_type_size: {event_type_size}; '
                   f'matches_count: {matches_count}')

    event_types = get_event_types(event_type_size, matches_count)
    subscription = cls(event_types)

    with duration(description):
        for event_type in event_types:
            subscription.matches(event_type)
