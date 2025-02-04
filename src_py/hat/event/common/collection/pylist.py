from hat.event.common.collection import common


class PyListEventTypeCollection(common.EventTypeCollection):

    def __init__(self, items=[]):
        self._values = {}

        for subscription, value in items:
            self.add(subscription, value)

    def add(self, subscription, value):
        self._values[value] = (self._values[value].union(subscription)
                               if value in self._values else subscription)

    def remove(self, value):
        self._values.pop(value, None)

    def get(self, event_type):
        for value, subscription in self._values.items():
            if subscription.matches(event_type):
                yield value
