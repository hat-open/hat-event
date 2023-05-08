import pytest

from hat.event.server import common
import hat.event.common.subscription


subscription_classes = [hat.event.common.subscription.PySubscription,
                        hat.event.common.subscription.CSubscription]


@pytest.mark.parametrize("event_type, query_type, is_match", [
    ((),
     (),
     True),

    ((),
     ('*',),
     True),

    ((),
     ('?',),
     False),

    (('a',),
     (),
     False),

    (('a',),
     ('*',),
     True),

    (('a',),
     ('?',),
     True),

    (('a',),
     ('?', '*'),
     True),

    (('a',),
     ('?', '?'),
     False),

    (('a',),
     ('a',),
     True),

    (('a',),
     ('a', '*'),
     True),

    (('a'),
     ('a', '?'),
     False),

    (('a',),
     ('b',),
     False),

    (('a',),
     ('a', 'b'),
     False),

    (('a', 'b'),
     (),
     False),

    (('a', 'b'),
     ('*',),
     True),

    (('a', 'b'),
     ('?',),
     False),

    (('a', 'b'),
     ('?', '*'),
     True),

    (('a', 'b'),
     ('?', '?'),
     True),

    (('a', 'b'),
     ('?', '?', '*'),
     True),

    (('a', 'b'),
     ('?', '?', '?'),
     False),

    (('a', 'b'),
     ('a',),
     False),

    (('a', 'b'),
     ('a', '*'),
     True),

    (('a', 'b'),
     ('a', '?'),
     True),

    (('a', 'b'),
     ('a', '?', '*'),
     True),

    (('a', 'b'),
     ('a', '?', '?'),
     False),

    (('a', 'b'),
     ('?', 'b'),
     True),

    (('a', 'b'),
     ('?', 'b', '*'),
     True),

    (('a', 'b'),
     ('?', 'b', '?'),
     False),

    (('a', 'b'),
     ('b', 'a'),
     False),

    (('a', 'b'),
     ('a', 'b', 'c'),
     False)
])
def test_matches_query_type(event_type, query_type, is_match):
    result = common.matches_query_type(event_type, query_type)
    assert result is is_match


@pytest.mark.parametrize("Subscription", subscription_classes)
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
])
def test_subscription_get_query_types(Subscription, query_types, sanitized):
    subscription = Subscription(query_types)
    result = list(subscription.get_query_types())
    assert len(result) == len(sanitized)
    assert {tuple(i) for i in result} == {tuple(i) for i in sanitized}


@pytest.mark.parametrize("Subscription", subscription_classes)
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
     [('a',), ('a', 'b')],
     [(), ('a', 'b', 'c'), ('b',)]),
])
def test_subscription_matches(Subscription, query_types, matching,
                              not_matching):
    subscription = Subscription(query_types)
    for i in matching:
        assert subscription.matches(i) is True
    for i in not_matching:
        assert subscription.matches(i) is False


@pytest.mark.parametrize("Subscription", subscription_classes)
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
])
def test_subscription_union(Subscription, query_types, union):
    subscription = Subscription([]).union(*(Subscription(i)
                                            for i in query_types))
    result = subscription.get_query_types()
    assert set(result) == set(union)


@pytest.mark.parametrize("Subscription", subscription_classes)
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
])
def test_subscription_isdisjoint(Subscription, first, second, isdisjoint):
    first = Subscription(first)
    second = Subscription(second)
    result = first.isdisjoint(second)
    assert result is isdisjoint


@pytest.mark.parametrize("Subscription", subscription_classes)
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
])
def test_subscription_intersection(Subscription, subscription_types,
                                   other_types, intersection_types):
    subscription = Subscription(subscription_types)
    other = Subscription(other_types)
    intersection = subscription.intersection(other)

    assert set(intersection.get_query_types()) == set(intersection_types)
