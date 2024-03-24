import itertools
import typing

import lmdb

from hat.event.backends.lmdb import common
from hat.event.backends.lmdb import environment
from hat.event.backends.lmdb.conditions import Conditions


class Changes(typing.NamedTuple):
    data: dict[common.EventTypeRef, common.Event]
    types: dict[common.EventTypeRef, common.EventId]


class AddResult(typing.NamedTuple):
    added_ref: common.EventRef | None
    removed_ref: tuple[common.EventId, common.EventRef] | None


def ext_create(env: environment.Environment,
               txn: lmdb.Transaction,
               conditions: Conditions,
               subscription: common.Subscription
               ) -> 'LatestDb':
    db = LatestDb()
    db._env = env
    db._subscription = subscription
    db._changes = Changes({}, {})

    db._event_type_refs = {
        event_type: ref
        for ref, event_type in env.ext_read(txn, common.DbType.LATEST_TYPE)}

    db._events = {
        event.type: event
        for ref, event in env.ext_read(txn, common.DbType.LATEST_DATA)
        if conditions.matches(event) and
        subscription.matches(event.type) and
        db._event_type_refs.get(event.type) == ref}

    db._next_event_type_refs = itertools.count(
        max(db._event_type_refs.values(), default=0) + 1)

    return db


class LatestDb:

    def add(self,
            event: common.Event
            ) -> AddResult:
        if not self._subscription.matches(event.type):
            return AddResult(added_ref=None,
                             removed_ref=None)

        previous_event = self._events.get(event.type)
        if previous_event and previous_event > event:
            return AddResult(added_ref=None,
                             removed_ref=None)

        event_type_ref = self._event_type_refs.get(event.type)
        if event_type_ref is None:
            event_type_ref = next(self._next_event_type_refs)
            self._changes.types[event_type_ref] = event.type
            self._event_type_refs[event.type] = event_type_ref

        self._changes.data[event_type_ref] = event
        self._events[event.type] = event

        added_ref = common.LatestEventRef(event_type_ref)
        removed_ref = ((previous_event.id, added_ref)
                       if previous_event else None)
        return AddResult(added_ref=added_ref,
                         removed_ref=removed_ref)

    def query(self,
              params: common.QueryLatestParams
              ) -> common.QueryResult:
        event_types = (set(params.event_types)
                       if params.event_types is not None
                       else None)

        if event_types is None or ('*', ) in event_types:
            events = self._events.values()

        elif any('*' in event_type or '?' in event_type
                 for event_type in event_types):
            subscription = common.create_subscription(event_types)
            events = (event for event in self._events.values()
                      if subscription.matches(event.type))

        elif len(event_types) < len(self._events):
            events = (self._events.get(event_type)
                      for event_type in event_types)
            events = (event for event in events if event)

        else:
            events = (event for event in self._events.values()
                      if event.type in event_types)

        return common.QueryResult(events=list(events),
                                  more_follows=False)

    def create_changes(self) -> Changes:
        changes, self._changes = self._changes, Changes({}, {})
        return changes

    def ext_write(self,
                  txn: lmdb.Transaction,
                  changes: Changes):
        self._env.ext_write(txn, common.DbType.LATEST_DATA,
                            changes.data.items())

        self._env.ext_write(txn, common.DbType.LATEST_TYPE,
                            changes.types.items())
