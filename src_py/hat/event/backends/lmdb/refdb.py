import collections
import itertools
import typing

import lmdb

from hat import util

from hat.event.backends.lmdb import common
from hat.event.backends.lmdb import environment


# depending on dict order for added refs
class ServerChanges(typing.NamedTuple):
    added: dict[common.EventId,
                tuple[common.Event,
                      set[common.EventRef]]]
    removed: dict[common.EventId,
                  set[common.EventRef]]


Changes: typing.TypeAlias = dict[common.ServerId, ServerChanges]


class RefDb:

    def __init__(self,
                 env: environment.Environment,
                 max_results: int = 4096):
        self._env = env
        self._max_results = max_results
        self._last_event_ids = {}
        self._changes = collections.defaultdict(_create_server_changes)

    def add(self,
            event: common.Event,
            refs: typing.Iterable[common.EventRef]):
        last_event_id = self._last_event_ids.get(event.id.server)
        if last_event_id and last_event_id > event.id:
            raise Exception('event older than last')

        self._last_event_ids[event.id.server] = event.id

        server_changes = self._changes[event.id.server]
        server_changes.added[event.id] = event, set(refs)

    def remove(self,
               event_id: common.EventId,
               ref: common.EventRef):
        server_changes = self._changes[event_id.server]
        added = server_changes.added.get(event_id)

        if added and ref in added[1]:
            added[1].remove(ref)

            if not added[1]:
                server_changes.added.pop(event_id)

            return

        removed = server_changes.removed[event_id]
        removed.add(ref)

    async def query(self,
                    params: common.QueryServerParams
                    ) -> common.QueryResult:
        if (params.last_event_id and
                params.last_event_id.server != params.server_id):
            raise ValueError('invalid server id')

        max_results = (params.max_results
                       if params.max_results is not None and
                       params.max_results < self._max_results
                       else self._max_results)

        changes = self._changes
        events = await self._env.execute(_ext_query_events, self._env,
                                         params.server_id, max_results + 1,
                                         params.last_event_id)

        if not params.persisted and len(events) <= max_results:
            last_event_id = events[-1].id if events else params.last_event_id
            changes_max_result = max_results + 1 - len(events)
            changes_events = _query_changes(changes, params.server_id,
                                            changes_max_result, last_event_id)

            events.extend(changes_events)

        if len(events) > max_results:
            events = list(itertools.islice(events, max_results))
            more_follows = True

        else:
            events = list(events)
            more_follows = False

        return common.QueryResult(events=events,
                                  more_follows=more_follows)

    def create_changes(self) -> Changes:
        self._changes, changes = (
            collections.defaultdict(_create_server_changes), self._changes)
        return changes

    def ext_write(self,
                  txn: lmdb.Transaction,
                  changes: Changes):
        db_def = common.db_defs[common.DbType.REF]

        with self._env.ext_cursor(txn, common.DbType.REF) as cursor:
            for server_changes in changes.values():
                event_ids = {*server_changes.added.keys(),
                             *server_changes.removed.keys()}

                for event_id in event_ids:
                    encoded_key = db_def.encode_key(event_id)
                    encoded_value = cursor.pop(encoded_key)
                    value = (db_def.decode_value(encoded_value)
                             if encoded_value else set())

                    added = server_changes.added.get(event_id)
                    if added:
                        value.update(added[1])

                    removed = server_changes.removed.get(event_id)
                    if removed:
                        value.difference_update(removed)

                    if not value:
                        continue

                    encoded_value = db_def.encode_value(value)
                    cursor.put(encoded_key, encoded_value)

    def ext_cleanup(self,
                    txn: lmdb.Transaction,
                    refs: typing.Iterable[tuple[common.EventId,
                                                common.EventRef]]):
        db_def = common.db_defs[common.DbType.REF]

        with self._env.ext_cursor(txn, common.DbType.REF) as cursor:
            for event_id, ref in refs:
                encoded_key = db_def.encode_key(event_id)
                encoded_value = cursor.pop(encoded_key)
                if not encoded_value:
                    continue

                value = db_def.decode_value(encoded_value)
                value.discard(ref)

                if not value:
                    continue

                encoded_value = db_def.encode_value(value)
                cursor.put(encoded_key, encoded_value)


def _query_changes(changes, server_id, max_results, last_event_id):
    server_changes = changes.get(server_id)
    if not server_changes:
        return []

    events = (event for _, (event, __) in server_changes.added.items())

    if last_event_id:
        events = itertools.dropwhile(lambda i: i.id <= last_event_id,
                                     events)

    return itertools.islice(events, max_results)


def _ext_query_events(env, server_id, max_results, last_event_id):
    db_def = common.db_defs[common.DbType.REF]

    start_key = (last_event_id if last_event_id
                 else common.EventId(server=server_id,
                                     session=0,
                                     instance=0))
    stop_key = common.EventId(server=server_id + 1,
                              session=0,
                              instance=0)

    encoded_start_key = db_def.encode_key(start_key)
    encoded_stop_key = db_def.encode_key(stop_key)

    events = collections.deque()

    with env.ext_begin() as txn:
        with env.ext_cursor(txn, common.DbType.REF) as cursor:
            available = cursor.set_range(encoded_start_key)
            if available and bytes(cursor.key()) == encoded_start_key:
                available = cursor.next()

            while (available and
                    len(events) < max_results and
                    bytes(cursor.key()) < encoded_stop_key):
                value = db_def.decode_value(cursor.value())
                ref = util.first(value)
                if not ref:
                    continue

                event = _ext_get_event(env, txn, ref)
                if event:
                    # TODO decode key and check event_id == event.id
                    events.append(event)

                available = cursor.next()

    return events


def _ext_get_event(env, txn, ref):
    if isinstance(ref, common.LatestEventRef):
        db_type = common.DbType.LATEST_DATA

    elif isinstance(ref, common.TimeseriesEventRef):
        db_type = common.DbType.TIMESERIES_DATA

    else:
        raise ValueError('unsupported event reference type')

    db_def = common.db_defs[db_type]
    encoded_key = db_def.encode_key(ref.key)

    with env.ext_cursor(txn, db_type) as cursor:
        encoded_value = cursor.get(encoded_key)
        if not encoded_value:
            return

        return db_def.decode_value(encoded_value)


def _create_server_changes():
    return ServerChanges({}, collections.defaultdict(set))
