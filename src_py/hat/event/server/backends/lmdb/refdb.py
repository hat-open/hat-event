import asyncio
import collections
import typing

import lmdb

from hat import aio
from hat import util
from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import encoder


query_queue_size = 100


async def create(executor: aio.Executor,
                 env: lmdb.Environment
                 ) -> 'RefDb':
    return await executor(_ext_create, executor, env)


def _ext_create(executor, env):
    db = RefDb()
    db._executor = executor
    db._env = env

    db._ref_db = common.ext_open_db(env, common.DbType.REF)
    db._latest_db = common.ext_open_db(env, common.DbType.LATEST_DATA)
    db._ordered_db = common.ext_open_db(env, common.DbType.ORDERED_DATA)

    return db


class RefDb(common.Flushable):

    async def query(self,
                    event_id: common.EventId
                    ) -> typing.AsyncIterable[typing.List[common.Event]]:

        # TODO use other executor to prevent blocking of other query and flush
        #      operations

        queue = aio.Queue(query_queue_size)
        loop = asyncio.get_running_loop()

        try:
            async with aio.Group() as async_group:
                async_group.spawn(self._executor, self._ext_query, event_id,
                                  queue, loop)

                async for events in queue:
                    yield events

        finally:
            queue.close()

    def create_ext_flush(self) -> common.ExtFlushCb:
        return self._ext_flush

    def _ext_query(self, event_id, queue, loop):
        try:
            start_key = event_id
            stop_key = common.EventId(server=event_id.server + 1,
                                      session=0,
                                      instance=0)

            encoded_start_key = encoder.encode_ref_db_key(start_key)
            encoded_stop_key = encoder.encode_ref_db_key(stop_key)

            with self._env.begin(db=self._ref_db, buffers=True) as txn:
                cursor = txn.cursor()

                available = cursor.set_range(encoded_start_key)
                if available and bytes(cursor.key()) == encoded_start_key:
                    available = cursor.next()

                events = collections.deque()

                while available and bytes(cursor.key()) < encoded_stop_key:
                    value = encoder.decode_ref_db_value(cursor.value())
                    ref = util.first(value)
                    if not ref:
                        continue

                    event = self._ext_get_event(ref)
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

    def _ext_flush(self, ctx):
        with self._env.begin(db=self._ref_db,
                             parent=ctx.transaction,
                             write=True) as txn:
            for change in ctx.get_changes():
                key = change.event_id
                encoded_key = encoder.encode_ref_db_key(key)

                encoded_value = txn.pop(encoded_key)
                value = (encoder.decode_ref_db_value(encoded_value)
                         if encoded_value else set())

                value = (value | change.added) - change.removed
                if not value:
                    continue
                encoded_value = encoder.encode_ref_db_value(value)

                txn.put(encoded_key, encoded_value)

    def _ext_get_event(self, ref):
        if isinstance(ref, common.LatestEventRef):
            db = self._latest_db
            encoded_key = encoder.encode_latest_data_db_key(ref.key)
            decode_value_fn = encoder.decode_latest_data_db_value

        elif isinstance(ref, common.OrderedEventRef):
            db = self._ordered_db
            encoded_key = encoder.encode_ordered_data_db_key(ref.key)
            decode_value_fn = encoder.decode_ordered_data_db_value

        else:
            raise ValueError('unssuported event reference type')

        with self._env.begin(db=db, buffers=True) as txn:
            encoded_value = txn.get(encoded_key)
            return decode_value_fn(encoded_value)
