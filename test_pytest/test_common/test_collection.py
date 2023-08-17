import pytest

from hat.event import common
import hat.event.common.collection


collection_classes = [hat.event.common.collection.ListEventTypeCollection,
                      hat.event.common.collection.TreeEventTypeCollection]


@pytest.mark.parametrize("EventTypeCollection", collection_classes)
def test_collection(EventTypeCollection):
    collection = EventTypeCollection()

    assert list(collection.get(tuple())) == []
    assert list(collection.get(('a', 'b', 'c'))) == []
    assert list(collection.get(('x', 'b', 'c'))) == []

    collection.add(common.Subscription([('*', )]), 1)
    collection.add(common.Subscription([]), 2)
    collection.add(common.Subscription([('?', 'b', 'c')]), 3)
    collection.add(common.Subscription([('a', 'b', 'c')]), 4)

    assert list(collection.get(tuple())) == [1]
    assert list(collection.get(('a', 'b', 'c'))) == [1, 3, 4]
    assert list(collection.get(('x', 'b', 'c'))) == [1, 3]

    collection.remove(3)

    assert list(collection.get(tuple())) == [1]
    assert list(collection.get(('a', 'b', 'c'))) == [1, 4]
    assert list(collection.get(('x', 'b', 'c'))) == [1]
