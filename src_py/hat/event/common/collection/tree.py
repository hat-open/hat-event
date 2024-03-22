import collections
import typing

from hat.event.common.collection import common


class TreeEventTypeCollection(common.EventTypeCollection):

    def __init__(self):
        self._root = _create_node()
        self._value_nodes = collections.defaultdict(collections.deque)

    def add(self, subscription, value):
        for query_type in subscription.get_query_types():
            node = self._root
            rest = query_type

            while rest:
                head, rest = rest[0], rest[1:]
                if head == '*' and rest:
                    raise ValueError('invalid query type')

                node = node.children[head]

            if value in node.values:
                return

            node.values.add(value)
            self._value_nodes[value].append(node)

    def remove(self, value):
        for node in self._value_nodes.pop(value, []):
            node.values.remove(value)

    def get(self, event_type):
        return set(_get(self._root, event_type))


class _Node(typing.NamedTuple):
    values: set
    children: collections.defaultdict[str, '_Node']


def _create_node():
    return _Node(set(), collections.defaultdict(_create_node))


def _get(node, event_type):
    if '*' in node.children:
        yield from node.children['*'].values

    if not event_type:
        yield from node.values
        return

    head, rest = event_type[0], event_type[1:]

    if head in node.children:
        yield from _get(node.children[head], rest)

    if '?' in node.children:
        yield from _get(node.children['?'], rest)
