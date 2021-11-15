import pytest

from hat.event.server import common


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
def test_subscription_get_query_types(query_types, sanitized):
    subscription = common.Subscription(query_types)
    result = list(subscription.get_query_types())
    assert len(result) == len(sanitized)
    assert {tuple(i) for i in result} == {tuple(i) for i in sanitized}


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
def test_subscription_matches(query_types, matching, not_matching):
    subscription = common.Subscription(query_types)
    for i in matching:
        assert subscription.matches(i) is True
    for i in not_matching:
        assert subscription.matches(i) is False


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
def test_subscription_union(query_types, union):
    subscription = common.Subscription([]).union(*(common.Subscription(i)
                                                   for i in query_types))
    result = subscription.get_query_types()
    assert set(result) == set(union)


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
def test_subscription_isdisjoint(first, second, isdisjoint):
    first = common.Subscription(first)
    second = common.Subscription(second)
    result = first.isdisjoint(second)
    assert result is isdisjoint
