import abc
import itertools
import typing

from hat.event.common.data import EventTypeSegment, EventType


def matches_query_type(event_type: EventType,
                       query_type: EventType
                       ) -> bool:
    """Determine if event type matches query type

    Event type is tested if it matches query type according to the following
    rules:

        * Matching is performed on subtypes in increasing order.
        * Event type is a match only if all its subtypes are matched by
          corresponding query subtypes.
        * Matching is finished when all query subtypes are exhausted.
        * Query subtype '?' matches exactly one event subtype of any value.
          The subtype must exist.
        * Query subtype '*' matches 0 or more event subtypes of any value. It
          must be the last query subtype.
        * All other values of query subtype match exactly one event subtype
          of the same value.
        * Query type without subtypes is matched only by event type with no
          subtypes.

    As a consequence of aforementioned matching rules, event subtypes '*' and
    '?' cannot be directly matched and it is advisable not to use them in event
    types.

    """
    is_variable = bool(query_type and query_type[-1] == '*')
    if is_variable:
        query_type = query_type[:-1]

    if len(event_type) < len(query_type):
        return False

    if len(event_type) > len(query_type) and not is_variable:
        return False

    for i, j in zip(event_type, query_type):
        if j != '?' and i != j:
            return False

    return True


class BaseSubscription(abc.ABC):
    """Subscription defined by query event types"""

    @abc.abstractmethod
    def __init__(self, query_types: typing.Iterable[EventType]):
        pass

    @abc.abstractmethod
    def get_query_types(self) -> typing.Iterable[EventType]:
        """Calculate sanitized query event types"""

    @abc.abstractmethod
    def matches(self, event_type: EventType) -> bool:
        """Does `event_type` match subscription"""

    @abc.abstractmethod
    def union(self, *others: 'BaseSubscription') -> 'BaseSubscription':
        """Create new subscription including event types from this and
        other subscriptions."""

    @abc.abstractmethod
    def intersection(self, *others: 'BaseSubscription') -> 'BaseSubscription':
        """Create new subscription containing event types in common with
        other subscriptions."""

    @abc.abstractmethod
    def isdisjoint(self, other: 'BaseSubscription') -> bool:
        """Return ``True`` if this subscription has no event types in common
        with other subscription."""


class Node(typing.NamedTuple):
    """Subscription tree node"""
    is_leaf: bool
    children: typing.Dict[EventTypeSegment, 'Node']


def node_from_query_types(query_types: typing.Iterable[EventType]) -> Node:
    node = Node(False, {})

    for query_type in query_types:
        node = _add_query_type(node, query_type)

    return node


def node_to_query_types(node: Node) -> typing.Iterable[EventType]:
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
