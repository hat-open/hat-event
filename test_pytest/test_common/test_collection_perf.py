import pytest

from hat.event import common


pytestmark = pytest.mark.perf


collection_classes = [common.ListEventTypeCollection,
                      common.TreeEventTypeCollection]


@pytest.mark.parametrize("cls", collection_classes)
@pytest.mark.parametrize("timeseries_count", [1, 10, 100, 10_000])
def test_get_timeseries(profile, duration, cls, timeseries_count):

    description = (f'class: {cls.__name__}; '
                   f'timeseries_count: {timeseries_count}')

    event_types = [('a', 'b', 'c', str(i))
                   for i in range(timeseries_count)]

    collection = cls()

    for event_type in event_types:
        subscription = common.create_subscription([event_type])
        collection.add(subscription, object())

    with duration(description):
        # with profile(f"{cls.__name__}_{timeseries_count}"):
        for event_type in event_types:
            for i in collection.get(event_type):
                pass
