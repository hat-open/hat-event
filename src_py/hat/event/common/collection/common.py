import abc
import typing

from hat.event.common.data import EventType
from hat.event.common.subscription import Subscription


T = typing.TypeVar('T', bound=typing.Hashable)


class BaseEventTypeCollection(abc.ABC, typing.Generic[T]):

    @abc.abstractmethod
    def __init__(self):
        pass

    @abc.abstractmethod
    def add(self, subscription: Subscription, value: T):
        pass

    @abc.abstractmethod
    def remove(self, value: T):
        pass

    @abc.abstractmethod
    def get(self, event_type: EventType) -> typing.Iterable[T]:
        pass
