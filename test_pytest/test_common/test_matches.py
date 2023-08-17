import pytest

from hat.event import common


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
