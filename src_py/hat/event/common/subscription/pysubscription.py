import itertools
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
        result._root = self._root

        for other in others:
            other = _convert_other(other)
            result._root = _union(result._root, other._root)

        return result

    def intersection(self, *others):
        result = PySubscription([])
        result._root = self._root

        for other in others:
            other = _convert_other(other)
            result._root = _intersection(result._root, other._root)

        return result

    def isdisjoint(self, other):
        other = _convert_other(other)
        return _isdisjoint(self._root, other._root)


def _convert_other(other):
    return (other if isinstance(other, PySubscription)
            else PySubscription(other.get_query_types()))


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


def _union(first_node, second_node):
    first_is_leaf, first_children = first_node
    second_is_leaf, second_children = second_node

    is_leaf = first_is_leaf or second_is_leaf

    if '*' in first_children or '*' in second_children:
        return _Node(is_leaf, {'*': (True, {})})

    children = {}

    for name, child in itertools.chain(first_children.items(),
                                       second_children.items()):
        prev_child = children.get(name)
        children[name] = _union(prev_child, child) if prev_child else child

    return _Node(is_leaf, children)


def _intersection(first_node, second_node):
    first_is_leaf, first_children = first_node
    second_is_leaf, second_children = second_node

    is_leaf = first_is_leaf and second_is_leaf

    if '*' in first_children:
        children = second_children

    elif '*' in second_children:
        children = first_children

    else:
        children = {}

        for name, child, other_children in itertools.chain(
                ((name, child, second_children)
                 for name, child in first_children.items()),
                ((name, child, first_children)
                 for name, child in second_children.items())):

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
                result_children = _intersection(child, other_child)
                prev_result_children = children.get(result_name)
                children[result_name] = (
                    _union(prev_result_children, result_children)
                    if prev_result_children else result_children)

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
