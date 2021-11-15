import collections
import math

import pytest

from hat.event import common


pytestmark = pytest.mark.perf


@pytest.mark.parametrize("event_type_size", [1, 3, 5, 10])
@pytest.mark.parametrize("matches_count", [1, 100, 10000, 1000000])
def test_matches_duration(duration, event_type_size, matches_count):
    event_types = collections.deque()
    base = (math.ceil(math.log(matches_count, event_type_size))
            if event_type_size > 1 else matches_count)
    if base < 1:
        base = 1
    for i in range(matches_count):
        event_type = collections.deque()
        rest = i
        for j in range(event_type_size - 1, -1, -1):
            digit = rest // (base ** j)
            rest = rest - digit * (base ** j)
            event_type.append(f'segment {digit}')
        event_types.append(tuple(event_type))

    subscription = common.Subscription(event_types)

    description = (f'subscription matches - '
                   f'event_type_size: {event_type_size}; '
                   f'matches_count: {matches_count}')

    with duration(description):
        for event_type in event_types:
            subscription.matches(event_type)
