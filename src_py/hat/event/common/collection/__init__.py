from collections.abc import Hashable, Iterable
import typing

from hat.event.common.collection.common import EventTypeCollection
from hat.event.common.collection.pytree import PyTreeEventTypeCollection
from hat.event.common.subscription import Subscription

try:
    from hat.event.common.collection.ctree import CTreeEventTypeCollection

except ImportError:
    CTreeEventTypeCollection = None


__all__ = ['EventTypeCollection',
           'create_event_type_collection']


T = typing.TypeVar('T', bound=Hashable)


def create_event_type_collection(items: Iterable[Subscription, T] = []
                                 ) -> EventTypeCollection[T]:
    if CTreeEventTypeCollection is not None:
        return CTreeEventTypeCollection(items)

    return PyTreeEventTypeCollection(items)
