import pytest

from hat.event.common.subscription.pysubscription import PySubscription
from hat.event.common.subscription.csubscription import CSubscription


subscription_classes = [PySubscription, CSubscription]


@pytest.mark.parametrize("cls", subscription_classes)
@pytest.mark.parametrize("query_types, sanitized", [
    ([],
     []),

    ([()],
     [()]),

    ([('a',), ('b',), ('c',)],
     [('a',), ('b',), ('c',)]),

    ([('a',), ('b', '*'), (), ('*',)],
     [('*',)]),

    ([('a', '*'), ('a',)],
     [('a', '*')]),

    ([('a', '*'), ('b',), ('c', '?'), ('c', '*')],
     [('a', '*'), ('b',), ('c', '*')]),

    ([('',), ('a',)],
     [('',), ('a',)]),

    ([('',), ('*',)],
     [('*',)]),
])
def test_subscription_get_query_types(cls, query_types, sanitized):
    subscription = cls(query_types)
    result = list(subscription.get_query_types())
    assert len(result) == len(sanitized)
    assert {tuple(i) for i in result} == {tuple(i) for i in sanitized}


@pytest.mark.parametrize("cls", subscription_classes)
@pytest.mark.parametrize("query_types, matching, not_matching", [
    ([],
     [],
     [('a',), ('a', 'b'), ()]),

    ([()],
     [()],
     [('a',), ('a', 'b')]),

    ([('*',)],
     [(), ('a',), ('a', 'b')],
     []),

    ([('a',)],
     [('a',)],
     [(), ('a', 'b'), ('b',)]),

    ([('a', '?'), ('a',)],
     [('a',), ('a', 'b'), ('a', '')],
     [(), ('a', 'b', 'c'), ('b',), ('',)]),
])
def test_subscription_matches(cls, query_types, matching,
                              not_matching):
    subscription = cls(query_types)
    for i in matching:
        assert subscription.matches(i) is True
    for i in not_matching:
        assert subscription.matches(i) is False


@pytest.mark.parametrize("cls", subscription_classes)
@pytest.mark.parametrize("query_types, union", [
    ([],
     []),

    ([[]],
     []),

    ([[('a',)], [('b',)]],
     [('a',), ('b',)]),

    ([[('a',)], [('b',)], [('*',)]],
     [('*',)]),

    ([[('a', 'b')], [('a', 'c')]],
     [('a', 'b'), ('a', 'c')]),

    ([[('a', '')], [('', 'a')]],
     [('a', ''), ('', 'a')]),
])
def test_subscription_union(cls, query_types, union):
    subscription = cls([]).union(*(cls(i) for i in query_types))
    result = subscription.get_query_types()
    assert set(result) == set(union)


@pytest.mark.parametrize("cls", subscription_classes)
@pytest.mark.parametrize("first, second, isdisjoint", [
    ([],
     [],
     True),

    ([('a',)],
     [('b',)],
     True),

    ([('a',)],
     [('a',)],
     False),

    ([('a',)],
     [('?',)],
     False),

    ([('?',)],
     [('?',)],
     False),

    ([('?', 'a')],
     [('?', 'b')],
     True),

    ([('a', 'b')],
     [('*',)],
     False),

    ([('a', 'b', '*')],
     [('a', 'b')],
     False),

    ([('a', '', '?')],
     [('a', '', 'b')],
     False),
])
def test_subscription_isdisjoint(cls, first, second, isdisjoint):
    first = cls(first)
    second = cls(second)
    result = first.isdisjoint(second)
    assert result is isdisjoint


@pytest.mark.parametrize("cls", subscription_classes)
@pytest.mark.parametrize("subscription_types, other_types, intersection_types", [  # NOQA
    ([],
     [],
     []),

    ([('a',)],
     [('b',)],
     []),

    ([('*',)],
     [('a',), ('b',), ('c',)],
     [('a',), ('b',), ('c',)]),

    ([('a',), ('b',)],
     [('b',), ('c',)],
     [('b',)]),

    ([('?', 'b')],
     [('a', 'b')],
     [('a', 'b')]),

    ([('',), ('a',)],
     [('',), ('b',)],
     [('',)]),
])
def test_subscription_intersection(cls, subscription_types,
                                   other_types, intersection_types):
    subscription = cls(subscription_types)
    other = cls(other_types)
    intersection = subscription.intersection(other)

    assert set(intersection.get_query_types()) == set(intersection_types)
