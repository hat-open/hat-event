
from hat.event.common.collection import common

from hat.event.common.collection import _ctree


# WIP
raise ImportError()


class CTreeEventTypeCollection(common.EventTypeCollection):

    def __init__(self, items=[]):
        self._collection = _ctree.Collection()

        for subscription, value in items:
            self._collection.add(subscription, value)

    def add(self, subscription, value):
        self._collection.add(subscription, value)

    def remove(self, value):
        self._collection.remove(value)

    def get(self, event_type):
        return self._collection.get(event_type)
