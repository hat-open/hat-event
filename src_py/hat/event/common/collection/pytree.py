import collections
import typing

from hat.event.common.collection import common


class PyTreeEventTypeCollection(common.EventTypeCollection):

    def __init__(self, items=[]):
        self._root = _create_node()
        self._value_nodes = collections.defaultdict(collections.deque)

        for subscription, value in items:
            self.add(subscription, value)

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


def _get(node, event_type, event_type_index=0):
    if '*' in node.children:
        yield from node.children['*'].values

    if event_type_index >= len(event_type):
        yield from node.values
        return

    head = event_type[event_type_index]
    if head in node.children:
        yield from _get(node.children[head], event_type, event_type_index + 1)

    if '?' in node.children:
        yield from _get(node.children['?'], event_type, event_type_index + 1)
