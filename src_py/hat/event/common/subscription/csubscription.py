from hat.event.common.subscription import common

from hat.event.common.subscription import _csubscription


class CSubscription(common.Subscription):
    """C implementation of Subscription"""

    def __init__(self, query_types):
        self._subscription = _csubscription.Subscription(query_types)

    def get_query_types(self):
        return self._subscription.get_query_types()

    def matches(self, event_type):
        return self._subscription.matches(event_type)

    def union(self, *others):
        node = common.node_from_query_types(
            self._subscription.get_query_types())

        for other in others:
            other_node = common.node_from_query_types(other.get_query_types())
            node = common.union(node, other_node)

        return CSubscription(common.node_to_query_types(node))

    def intersection(self, *others):
        node = common.node_from_query_types(
            self._subscription.get_query_types())

        for other in others:
            other_node = common.node_from_query_types(other.get_query_types())
            node = common.intersection(node, other_node)

        return CSubscription(common.node_to_query_types(node))

    def isdisjoint(self, other):
        other_subscription = _get_other_subscription(other)
        return self._subscription.isdisjoint(other_subscription)


def _get_other_subscription(other):
    if isinstance(other, CSubscription):
        return other._subscription

    return CSubscription(other.get_query_types())._subscription
