from collections.abc import Hashable, Iterable
import typing

from hat.event.common.collection.common import EventTypeCollection
from hat.event.common.collection.list import ListEventTypeCollection
from hat.event.common.collection.tree import TreeEventTypeCollection
from hat.event.common.subscription import Subscription


__all__ = ['EventTypeCollection',
           'ListEventTypeCollection',
           'TreeEventTypeCollection',
           'create_event_type_collection']


T = typing.TypeVar('T', bound=Hashable)


def create_event_type_collection(items: Iterable[Subscription, T] = []
                                 ) -> EventTypeCollection[T]:
    collection = TreeEventTypeCollection()

    for subscription, value in items:
        collection.add(subscription, value)

    return collection
