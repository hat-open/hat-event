from collections.abc import Iterable
import abc
import itertools
import typing

from hat.event.common.common import EventTypeSegment, EventType


class Subscription(abc.ABC):
    """Subscription defined by query event types"""

    @abc.abstractmethod
    def __init__(self, query_types: Iterable[EventType]):
        """Create subscription instance"""

    @abc.abstractmethod
    def get_query_types(self) -> Iterable[EventType]:
        """Calculate sanitized query event types"""

    @abc.abstractmethod
    def matches(self, event_type: EventType) -> bool:
        """Does `event_type` match subscription"""

    @abc.abstractmethod
    def union(self, *others: 'Subscription') -> 'Subscription':
        """Create new subscription including event types from this and
        other subscriptions."""

    @abc.abstractmethod
    def intersection(self, *others: 'Subscription') -> 'Subscription':
        """Create new subscription containing event types in common with
        other subscriptions."""

    @abc.abstractmethod
    def isdisjoint(self, other: 'Subscription') -> bool:
        """Return ``True`` if this subscription has no event types in common
        with other subscription."""


class Node(typing.NamedTuple):
    """Subscription tree node"""
    is_leaf: bool
    children: typing.Dict[EventTypeSegment, 'Node']


def node_from_query_types(query_types: Iterable[EventType]) -> Node:
    node = Node(False, {})

    for query_type in query_types:
        node = _add_query_type(node, query_type)

    return node


def node_to_query_types(node: Node) -> Iterable[EventType]:
    is_leaf, children = node

    if is_leaf and '*' not in children:
        yield ()

    for head, child in children.items():
        for rest in node_to_query_types(child):
            yield (head, *rest)


def union(first: Node, second: Node) -> Node:
    is_leaf = first.is_leaf or second.is_leaf

    if '*' in first.children or '*' in second.children:
        return Node(is_leaf, {'*': (True, {})})

    children = {}

    for name, child in itertools.chain(first.children.items(),
                                       second.children.items()):
        prev_child = children.get(name)
        children[name] = union(prev_child, child) if prev_child else child

    return Node(is_leaf, children)


def intersection(first: Node, second: Node) -> Node:
    is_leaf = first.is_leaf and second.is_leaf

    if '*' in first.children:
        children = second.children

    elif '*' in second.children:
        children = first.children

    else:
        children = {}

        for name, child, other_children in itertools.chain(
                ((name, child, second.children)
                 for name, child in first.children.items()),
                ((name, child, first.children)
                 for name, child in second.children.items())):

            if name == '?':
                candidates = other_children

            else:
                candidates = {}
                other_child = other_children.get(name)
                if other_child:
                    candidates[name] = other_child
                other_child = other_children.get('?')
                if other_child:
                    candidates['?'] = other_child

            for other_name, other_child in candidates.items():
                result_name = name if name != '?' else other_name
                result_children = intersection(child, other_child)
                prev_result_children = children.get(result_name)
                children[result_name] = (
                    union(prev_result_children, result_children)
                    if prev_result_children else result_children)

    return Node(is_leaf, children)


def _add_query_type(node, query_type):
    is_leaf, children = node

    if '*' in children:
        return node

    if not query_type:
        return Node(True, children)

    head, rest = query_type[0], query_type[1:]

    if head == '*':
        if rest:
            raise ValueError('invalid query event type')
        children.clear()
        children['*'] = Node(True, {})

    else:
        child = children.get(head) or Node(False, {})
        child = _add_query_type(child, rest)
        children[head] = child

    return node
