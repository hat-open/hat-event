import typing

from hat.event.common.collection.common import BaseEventTypeCollection
from hat.event.common.collection.list import ListEventTypeCollection
from hat.event.common.collection.tree import TreeEventTypeCollection


__all__ = ['BaseEventTypeCollection',
           'ListEventTypeCollection',
           'TreeEventTypeCollection',
           'EventTypeCollection']


EventTypeCollection: typing.TypeAlias = TreeEventTypeCollection
