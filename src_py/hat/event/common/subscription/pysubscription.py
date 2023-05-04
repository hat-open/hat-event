import collections
import typing

from hat.event.common.data import EventTypeSegment
from hat.event.common.subscription.common import BaseSubscription


class PySubscription(BaseSubscription):
    """Python implementation of Subscription"""

    def __init__(self, query_types):
        self._root = _Node(False, {})
        for query_type in query_types:
            self._root = _add_query_type(self._root, query_type)

    def get_query_types(self):
        yield from _get_query_types(self._root)

    def matches(self, event_type):
        return _matches(self._root, event_type, 0)

    def union(self, *others):
        result = PySubscription([])
        other_roots = ((other if isinstance(other, PySubscription)
                        else PySubscription(other.get_query_types()))._root
                       for other in others)
        result._root = _union([self._root, *other_roots])
        return result

    def isdisjoint(self, other):
        other_root = (other if isinstance(other, PySubscription)
                      else PySubscription(other.get_query_types()))._root
        return _isdisjoint(self._root, other_root)


class _Node(typing.NamedTuple):
    is_leaf: bool
    children: typing.Dict[EventTypeSegment, '_Node']


def _add_query_type(node, query_type):
    is_leaf, children = node

    if '*' in children:
        return node

    if not query_type:
        return _Node(True, children)

    head, rest = query_type[0], query_type[1:]

    if head == '*':
        if rest:
            raise ValueError('invalid query event type')
        children.clear()
        children['*'] = True, {}

    else:
        child = children.get(head, (False, {}))
        child = _add_query_type(child, rest)
        children[head] = child

    return node


def _get_query_types(node):
    is_leaf, children = node

    if is_leaf and '*' not in children:
        yield ()

    for head, child in children.items():
        for rest in _get_query_types(child):
            yield (head, *rest)


def _matches(node, event_type, event_type_index):
    is_leaf, children = node

    if '*' in children:
        return True

    if event_type_index >= len(event_type):
        return is_leaf

    child = children.get(event_type[event_type_index])
    if child and _matches(child, event_type, event_type_index + 1):
        return True

    child = children.get('?')
    if child and _matches(child, event_type, event_type_index + 1):
        return True

    return False


def _union(nodes):
    if len(nodes) < 2:
        return nodes[0]

    is_leaf = any(i for i, _ in nodes)

    names = {}
    for _, node_children in nodes:
        for name, node_child in node_children.items():
            if name == '*':
                return _Node(is_leaf, {'*': (True, {})})
            if name not in names:
                names[name] = collections.deque()
            names[name].append(node_child)

    children = {name: _union(named_children)
                for name, named_children in names.items()}

    return _Node(is_leaf, children)


def _isdisjoint(first_node, second_node):
    first_is_leaf, first_children = first_node
    second_is_leaf, second_children = second_node

    if first_is_leaf and second_is_leaf:
        return False

    if (('*' in first_children and second_children) or
            ('*' in second_children and first_children)):
        return False

    if '?' in first_children:
        for child in second_children.values():
            if not _isdisjoint(first_children['?'], child):
                return False

    if '?' in second_children:
        for name, child in first_children.items():
            if name == '?':
                continue
            if not _isdisjoint(second_children['?'], child):
                return False

    names = set(first_children.keys()).intersection(second_children.keys())
    for name in names:
        if name == '?':
            continue
        if not _isdisjoint(first_children[name], second_children[name]):
            return False

    return True
