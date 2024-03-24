from collections.abc import Iterable

from hat.event.common.common import EventType
from hat.event.common.subscription.common import Subscription
from hat.event.common.subscription.pysubscription import PySubscription

try:
    from hat.event.common.subscription.csubscription import CSubscription

except ImportError:
    CSubscription = None


__all__ = ['Subscription',
           'create_subscription']


def create_subscription(query_types: Iterable[EventType]) -> Subscription:
    if CSubscription is not None:
        return CSubscription(query_types)

    return PySubscription(query_types)
