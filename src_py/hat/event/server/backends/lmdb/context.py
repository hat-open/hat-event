import abc
import collections
import typing

import lmdb

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb.refdb import RefDb


ExtFlushCb = typing.Callable[['Context'], None]


class Flushable(abc.ABC):

    @abc.abstractmethod
    def create_ext_flush(self) -> ExtFlushCb:
        pass


class Context:

    def __init__(self,
                 tnx: lmdb.Transaction,
                 ref_db: RefDb):
        self._txn = tnx
        self._ref_db = ref_db
        self._timestamp = common.now()
        self._events = {}
        self._refs = {}

    @property
    def txn(self) -> lmdb.Transaction:
        return self._txn

    @property
    def timestamp(self) -> common.Timestamp:
        return self._timestamp

    def ext_add_event_ref(self,
                          event: common.Event,
                          ref: common.EventRef):
        self._events[event.event_id] = event
        self._refs.setdefault(event.event_id, set()).add(ref)

        self._ref_db.ext_add_event_ref(self._txn, event.event_id, ref)

    def ext_remove_event_ref(self,
                             event_id: common.EventId,
                             ref: common.EventRef):
        if event_id in self._events:
            refs = self._refs[event_id]
            refs.discard(ref)

            if not refs:
                del self._events[event_id]
                del self._refs[event_id]

        self._ref_db.ext_remove_event_ref(self._txn, event_id, ref)

    def get_events(self) -> typing.Iterable[typing.List[common.Event]]:
        events = collections.deque()
        session = collections.deque()

        for event_id in sorted(self._events.keys()):
            if session and session[0].event_id.session != event_id.session:
                events.append(list(session))
                session = collections.deque()
            session.append(self._events[event_id])

        if session:
            events.append(list(session))

        return events
