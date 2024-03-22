import pytest

from hat.event import common


collection_classes = [common.ListEventTypeCollection,
                      common.TreeEventTypeCollection]


@pytest.mark.parametrize("cls", collection_classes)
def test_collection(cls):
    collection = cls()

    assert list(collection.get(tuple())) == []
    assert list(collection.get(('a', 'b', 'c'))) == []
    assert list(collection.get(('x', 'b', 'c'))) == []

    collection.add(common.create_subscription([('*', )]), 1)
    collection.add(common.create_subscription([]), 2)
    collection.add(common.create_subscription([('?', 'b', 'c')]), 3)
    collection.add(common.create_subscription([('a', 'b', 'c')]), 4)

    assert list(collection.get(tuple())) == [1]
    assert list(collection.get(('a', 'b', 'c'))) == [1, 3, 4]
    assert list(collection.get(('x', 'b', 'c'))) == [1, 3]

    collection.remove(3)

    assert list(collection.get(tuple())) == [1]
    assert list(collection.get(('a', 'b', 'c'))) == [1, 4]
    assert list(collection.get(('x', 'b', 'c'))) == [1]
