import functools

import pytest

from hat.event import common
from hat.event.common.collection.pylist import PyListEventTypeCollection
from hat.event.common.collection.pytree import PyTreeEventTypeCollection


pytestmark = pytest.mark.perf


collection_classes = [PyListEventTypeCollection,
                      PyTreeEventTypeCollection]


@pytest.mark.parametrize("cls", collection_classes)
@pytest.mark.parametrize("timeseries_count", [1, 10, 100, 14_000])
@pytest.mark.parametrize("with_cache", [False, True])
def test_get_timeseries(duration, cls, timeseries_count, with_cache):

    description = (f'class: {cls.__name__}; '
                   f'timeseries_count: {timeseries_count}; '
                   f'with_cache: {with_cache}')

    event_types = [('a', 'b', 'c', str(i))
                   for i in range(timeseries_count)]

    collection = cls((common.create_subscription([event_type]), object())
                     for event_type in event_types)

    if with_cache:
        get_element = functools.lru_cache(maxsize=timeseries_count)(
            lambda event_type: list(collection.get(event_type)))

    else:
        get_element = collection.get

    with duration(f'first run - {description}'):
        for event_type in event_types:
            for i in get_element(event_type):
                pass

    with duration(f'second run - {description}'):
        for event_type in event_types:
            for i in get_element(event_type):
                pass
