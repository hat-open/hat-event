import collections

from hat.event.common.collection import common

from hat.event.common.collection import _ctree


class CTreeEventTypeCollection(common.EventTypeCollection):

    def __init__(self, items=[]):
        self._collection = _ctree.Collection()
        self._refs = collections.defaultdict(collections.deque)

        for subscription, value in items:
            self.add(subscription, value)

    def add(self, subscription, value):
        refs = self._refs[value]
        for query_type in subscription.get_query_types():
            ref = self._collection.add(query_type, value)
            refs.append(ref)

    def remove(self, value):
        for ref in self._refs.pop(value, []):
            self._collection.remove(ref)

    def get(self, event_type):
        return self._collection.get(event_type)
