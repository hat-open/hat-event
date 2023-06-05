from hat.event.common.subscription import common


class PySubscription(common.BaseSubscription):
    """Python implementation of Subscription"""

    def __init__(self, query_types):
        self._root = common.node_from_query_types(query_types)

    def get_query_types(self):
        return common.node_to_query_types(self._root)

    def matches(self, event_type):
        return _matches(self._root, event_type, 0)

    def union(self, *others):
        node = self._root

        for other in others:
            other_node = _get_other_node(other)
            node = common.union(node, other_node)

        result = PySubscription([])
        result._root = node
        return result

    def intersection(self, *others):
        node = self._root

        for other in others:
            other_node = _get_other_node(other)
            node = common.intersection(node, other_node)

        result = PySubscription([])
        result._root = node
        return result

    def isdisjoint(self, other):
        other_node = _get_other_node(other)
        return _isdisjoint(self._root, other_node)


def _get_other_node(other):
    if isinstance(other, PySubscription):
        return other._root

    return common.node_from_query_types(other.get_query_types())


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


def _isdisjoint(first, second):
    if first.is_leaf and second.is_leaf:
        return False

    if (('*' in first.children and (second.children or second.is_leaf)) or
            ('*' in second.children and (first.children or first.is_leaf))):
        return False

    if '?' in first.children:
        for child in second.children.values():
            if not _isdisjoint(first.children['?'], child):
                return False

    if '?' in second.children:
        for name, child in first.children.items():
            if name == '?':
                continue
            if not _isdisjoint(second.children['?'], child):
                return False

    names = set(first.children.keys()).intersection(second.children.keys())
    for name in names:
        if name == '?':
            continue
        if not _isdisjoint(first.children[name], second.children[name]):
            return False

    return True
