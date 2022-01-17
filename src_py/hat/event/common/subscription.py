import typing

from hat.event.common.data import EventType
from hat.event.common import _pysubscription

try:
    from hat.event.common import _csubscription
except ImportError:
    _csubscription = None


Subscription: typing.Type = (_csubscription.Subscription
                             if _csubscription
                             else _pysubscription.Subscription)
"""Default subscription implementation"""


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
