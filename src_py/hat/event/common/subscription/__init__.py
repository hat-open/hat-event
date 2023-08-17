import typing

from hat.event.common.subscription.common import BaseSubscription
from hat.event.common.subscription.pysubscription import PySubscription


__all__ = ['BaseSubscription',
           'Subscription',
           'PySubscription']

try:
    from hat.event.common.subscription.csubscription import CSubscription

    Subscription: typing.TypeAlias = CSubscription
    __all__ += ['CSubscription']

except ImportError:
    Subscription: typing.TypeAlias = PySubscription
