import asyncio
import collections
import typing

import lmdb

from hat import aio
from hat import util

from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder
from hat.event.server.backends.lmdb import environment


query_queue_size = 100


class RefDb:

    def __init__(self, env: environment.Environment):
        self._env = env

    async def query(self,
                    event_id: common.EventId
                    ) -> typing.AsyncIterable[list[common.Event]]:

        # TODO use other executor to prevent blocking of other query and flush
        #      operations

        queue = aio.Queue(query_queue_size)
        loop = asyncio.get_running_loop()

        try:
            self._env.async_group.spawn(self._env.execute, self._ext_query,
                                        event_id, queue, loop)

            async for events in queue:
                yield events

        finally:
            queue.close()

    def ext_add_event_ref(self,
                          txn: lmdb.Transaction,
                          event_id: common.EventId,
                          ref: common.EventRef):
        with self._env.ext_cursor(txn, common.DbType.REF) as cursor:
            encoded_key = encoder.encode_ref_db_key(event_id)

            encoded_value = cursor.pop(encoded_key)
            value = (encoder.decode_ref_db_value(encoded_value)
                     if encoded_value else set())

            value.add(ref)
            encoded_value = encoder.encode_ref_db_value(value)

            cursor.put(encoded_key, encoded_value)

    def ext_remove_event_ref(self,
                             txn: lmdb.Transaction,
                             event_id: common.EventId,
                             ref: common.EventRef):
        with self._env.ext_cursor(txn, common.DbType.REF) as cursor:
            encoded_key = encoder.encode_ref_db_key(event_id)

            encoded_value = cursor.pop(encoded_key)
            value = (encoder.decode_ref_db_value(encoded_value)
                     if encoded_value else set())

            value.discard(ref)
            if not value:
                return
            encoded_value = encoder.encode_ref_db_value(value)

            cursor.put(encoded_key, encoded_value)

    def _ext_query(self, event_id, queue, loop):
        try:
            start_key = event_id
            stop_key = common.EventId(server=event_id.server + 1,
                                      session=0,
                                      instance=0)

            encoded_start_key = encoder.encode_ref_db_key(start_key)
            encoded_stop_key = encoder.encode_ref_db_key(stop_key)

            with self._env.ext_begin() as txn:
                with self._env.ext_cursor(txn, common.DbType.REF) as cursor:
                    available = cursor.set_range(encoded_start_key)
                    if available and bytes(cursor.key()) == encoded_start_key:
                        available = cursor.next()

                    events = collections.deque()

                    while available and bytes(cursor.key()) < encoded_stop_key:
                        value = encoder.decode_ref_db_value(cursor.value())
                        ref = util.first(value)
                        if not ref:
                            continue

                        event = self._ext_get_event(txn, ref)
                        session = event.event_id.session

                        if events and events[0].event_id.session != session:
                            future = asyncio.run_coroutine_threadsafe(
                                queue.put(list(events)), loop)
                            future.result()
                            events = collections.deque()

                        events.append(event)
                        available = cursor.next()

                    if events:
                        future = asyncio.run_coroutine_threadsafe(
                            queue.put(list(events)), loop)
                        future.result()

        except aio.QueueClosedError:
            pass

        finally:
            loop.call_soon_threadsafe(queue.close)

    def _ext_get_event(self, txn, ref):
        if isinstance(ref, common.LatestEventRef):
            db_type = common.DbType.LATEST_DATA
            encoded_key = encoder.encode_latest_data_db_key(ref.key)
            decode_value_fn = encoder.decode_latest_data_db_value

        elif isinstance(ref, common.OrderedEventRef):
            db_type = common.DbType.ORDERED_DATA
            encoded_key = encoder.encode_ordered_data_db_key(ref.key)
            decode_value_fn = encoder.decode_ordered_data_db_value

        else:
            raise ValueError('unsupported event reference type')

        with self._env.ext_cursor(txn, db_type) as cursor:
            encoded_value = cursor.get(encoded_key)
            return decode_value_fn(encoded_value)
