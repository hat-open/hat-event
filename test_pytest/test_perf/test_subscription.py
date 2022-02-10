import collections
import math

import pytest

import hat.event.common._pysubscription
import hat.event.common._csubscription


pytestmark = pytest.mark.perf


subscription_classes = [hat.event.common._pysubscription.Subscription,
                        hat.event.common._csubscription.Subscription]


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


@pytest.mark.parametrize("Subscription", subscription_classes)
@pytest.mark.parametrize("event_type_size", [1, 3, 5, 10])
@pytest.mark.parametrize("event_type_count", [1, 100, 10000, 1000000])
def test_create(duration, Subscription, event_type_size, event_type_count):
    description = (f'subscription create - '
                   f'class: {Subscription}; '
                   f'event_type_size: {event_type_size}; '
                   f'event_type_count: {event_type_count}')

    event_types = get_event_types(event_type_size, event_type_count)

    with duration(description):
        Subscription(event_types)


@pytest.mark.parametrize("Subscription", subscription_classes)
@pytest.mark.parametrize("event_type_size", [1, 3, 5, 10])
@pytest.mark.parametrize("matches_count", [1, 100, 10000, 1000000])
def test_matches(duration, Subscription, event_type_size, matches_count):

    description = (f'subscription matches - '
                   f'class: {Subscription}; '
                   f'event_type_size: {event_type_size}; '
                   f'matches_count: {matches_count}')

    event_types = get_event_types(event_type_size, matches_count)
    subscription = Subscription(event_types)

    with duration(description):
        for event_type in event_types:
            subscription.matches(event_type)


@pytest.mark.parametrize("Subscription", subscription_classes)
@pytest.mark.parametrize("event_type_size", [1, 3, 5, 10])
@pytest.mark.parametrize("matches_count", [1, 100, 10000, 1000000])
def test_matches_with_cache(duration, Subscription, event_type_size,
                            matches_count):

    description = (f'subscription matches with cache - '
                   f'class: {Subscription}; '
                   f'event_type_size: {event_type_size}; '
                   f'matches_count: {matches_count}')

    event_types = get_event_types(event_type_size, matches_count)
    subscription = Subscription(event_types, cache_maxsize=None)
    for event_type in event_types:
        subscription.matches(event_type)

    event_types = get_event_types(event_type_size, matches_count)
    with duration(description):
        for event_type in event_types:
            subscription.matches(event_type)
