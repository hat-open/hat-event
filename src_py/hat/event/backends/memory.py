"""Simple memory backend

All registered events are stored in single unsorted continuous event list.

"""

import collections

from hat import aio

from hat.event import common


class MemoryBackend(common.Backend):

    def __init__(self, conf, registered_events_cb, flushed_events_cb):
        self._registered_events_cb = registered_events_cb
        self._flushed_events_cb = flushed_events_cb
        self._async_group = aio.Group()
        self._events = collections.deque()

    @property
    def async_group(self):
        return self._async_group

    async def get_last_event_id(self, server_id):
        event_ids = (e.event_id for e in self._events
                     if e.server == server_id)
        default = common.EventId(server=server_id, session=0, instance=0)
        return max(event_ids, default=default)

    async def register(self, events):
        self._events.extend(events)

        if self._registered_events_cb:
            await aio.call(self._registered_events_cb, events)

        if self._flushed_events_cb:
            await aio.call(self._flushed_events_cb, events)

        return events

    async def query(self, params):
        if isinstance(params, common.QueryLatestParams):
            return self._query_latest(params)

        if isinstance(params, common.QueryTimeseriesParams):
            return self._query_timeseries(params)

        if isinstance(params, common.QueryServerParams):
            return self._query_server(params)

        raise ValueError('unsupported params type')

    async def flush(self):
        pass

    def _query_latest(self, params):
        events = self._events

        if params.event_types is not None:
            events = _filter_event_types(events, params.event_types)

        result = {}
        for event in events:
            previous = result.get(event.type)
            if previous is None or previous < event:
                result[event.type] = event

        return common.QueryResult(events=list(result.values()),
                                  more_follows=False)

    def _query_timeseries(self, params):
        events = self._events

        if params.event_types is not None:
            events = _filter_event_types(events, params.event_types)

        if params.t_from is not None:
            events = _filter_t_from(events, params.t_from)

        if params.t_to is not None:
            events = _filter_t_to(events, params.t_to)

        if params.source_t_from is not None:
            events = _filter_source_t_from(events, params.source_t_from)

        if params.source_t_to is not None:
            events = _filter_source_t_to(events, params.source_t_to)

        if params.order_by == common.OrderBy.TIMESTAMP:
            sort_key = lambda event: event.timestamp, event  # NOQA
        elif params.order_by == common.OrderBy.SOURCE_TIMESTAMP:
            sort_key = lambda event: event.source_timestamp, event  # NOQA
        else:
            raise ValueError('invalid order by')

        if params.order == common.Order.ASCENDING:
            sort_reverse = False
        elif params.order == common.Order.DESCENDING:
            sort_reverse = True
        else:
            raise ValueError('invalid order by')

        events = sorted(events, key=sort_key, reverse=sort_reverse)

        if params.last_event_id and events:
            for i, event in enumerate(events):
                if event.id == params.last_event_id:
                    break
            events = events[i+1:]

        if params.max_results is not None and len(events) > params.max_results:
            more_follows = True
            events = events[:params.max_results]
        else:
            more_follows = False

        return common.QueryResult(events=events,
                                  more_follows=more_follows)

    def _query_server(self, params):
        events = sorted(_filter_server_id(self._events, params.server_id))

        if params.last_event_id and events:
            for i, event in enumerate(events):
                if event.id > params.last_event_id:
                    break
            events = events[i:]

        if params.max_results is not None and len(events) > params.max_results:
            more_follows = True
            events = events[:params.max_results]
        else:
            more_follows = False

        return common.QueryResult(events=events,
                                  more_follows=more_follows)


info = common.BackendInfo(MemoryBackend)


def _filter_event_types(events, event_types):
    subscription = common.create_subscription(event_types)
    for event in events:
        if subscription.matches(event.type):
            yield event


def _filter_t_from(events, t_from):
    for event in events:
        if event.timestamp >= t_from:
            yield event


def _filter_t_to(events, t_to):
    for event in events:
        if event.timestamp <= t_to:
            yield event


def _filter_source_t_from(events, source_t_from):
    for event in events:
        if event.source_timestamp >= source_t_from:
            yield event


def _filter_source_t_to(events, source_t_to):
    for event in events:
        if event.source_timestamp <= source_t_to:
            yield event


def _filter_server_id(events, server_id):
    for event in events:
        if event.event_id.server == server_id:
            yield event
