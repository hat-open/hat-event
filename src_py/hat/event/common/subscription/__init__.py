from hat.event.common.subscription.common import (matches_query_type,
                                                  BaseSubscription)
from hat.event.common.subscription.pysubscription import PySubscription


__all__ = ['matches_query_type',
           'BaseSubscription',
           'Subscription',
           'PySubscription']

try:
    from hat.event.common.subscription.csubscription import CSubscription

    Subscription = CSubscription
    __all__ += ['CSubscription']

except ImportError:
    Subscription = PySubscription
