from pathlib import Path
import asyncio
import logging
import typing

from hat import aio
from hat import json
from hat import util
from hat.event.server.backends.lmdb import common
from hat.event.server.backends.lmdb import context
from hat.event.server.backends.lmdb import latestdb
from hat.event.server.backends.lmdb import ordereddb
from hat.event.server.backends.lmdb import refdb
from hat.event.server.backends.lmdb import systemdb
from hat.event.server.backends.lmdb.conditions import Conditions


mlog = logging.getLogger(__name__)


async def create(conf: json.Data
                 ) -> 'LmdbBackend':
    backend = LmdbBackend()
    backend._env_closed = False
    backend._flush_lock = asyncio.Lock()
    backend._flush_period = conf['flush_period']
    backend._flushed_events_cbs = util.CallbackRegistry()
    backend._executor = aio.create_executor(1)

    backend._conditions = Conditions(conf['conditions'])

    await backend._executor(_ext_init, backend, conf)

    backend._async_group = aio.Group()
    backend._async_group.spawn(backend._write_loop)

    return backend


def _ext_init(backend, conf):
    backend._env = common.ext_create_env(path=Path(conf['db_path']),
                                         max_size=conf['max_db_size'])

    backend._ref_db = refdb.ext_create(executor=backend._executor,
                                       env=backend._env)

    with backend._env.begin(write=True) as txn:
        ctx = context.Context(txn=txn,
                              ref_db=backend._ref_db)

        backend._sys_db = systemdb.ext_create(env=backend._env,
                                              ctx=ctx)

        backend._latest_db = latestdb.ext_create(
            env=backend._env,
            ctx=ctx,
            subscription=common.Subscription(
                tuple(i) for i in conf['latest']['subscriptions']),
            conditions=backend._conditions)

        backend._ordered_dbs = [
            ordereddb.ext_create(
                executor=backend._executor,
                env=backend._env,
                ctx=ctx,
                subscription=common.Subscription(
                    tuple(et) for et in i['subscriptions']),
                conditions=backend._conditions,
                order_by=common.OrderBy[i['order_by']],
                limit=i.get('limit'))
            for i in conf['ordered']]

    # TODO: maybe cleanup unused ordered partitions
    # ordereddb.cleanup(
    #     {ordered_db.partition_id for ordered_db in backend._ordered_dbs})


class LmdbBackend(common.Backend):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    def register_flushed_events_cb(self,
                                   cb: typing.Callable[[typing.List[common.Event]],  # NOQA
                                                       None]
                                   ) -> util.RegisterCallbackHandle:
        return self._flushed_events_cbs.register(cb)

    async def get_last_event_id(self,
                                server_id: int
                                ) -> common.EventId:
        event_id, _ = self._sys_db.get_last_event_id_timestamp(server_id)
        return event_id

    async def register(self,
                       events: typing.List[common.Event]
                       ) -> typing.List[common.Event]:
        for event in events:
            last_event_id, last_timestamp = \
                self._sys_db.get_last_event_id_timestamp(event.event_id.server)

            if last_event_id >= event.event_id:
                mlog.warning("event registration skipped: invalid event id")
                continue

            if last_timestamp > event.timestamp:
                mlog.warning("event registration skipped: invalid timestamp")
                continue

            if not self._conditions.matches(event):
                mlog.warning("event registration skipped: invalid conditions")
                continue

            registered = False

            if self._latest_db.add(event):
                registered = True

            for db in self._ordered_dbs:
                if db.add(event):
                    registered = True

            if not registered:
                continue

            self._sys_db.set_last_event_id_timestamp(event.event_id,
                                                     event.timestamp)

        return events

    async def query(self,
                    data: common.QueryData
                    ) -> typing.List[common.Event]:
        if (data.server_id is None and
                data.event_ids is None and
                data.t_to is None and
                data.source_t_from is None and
                data.source_t_to is None and
                data.payload is None and
                data.order == common.Order.DESCENDING and
                data.order_by == common.OrderBy.TIMESTAMP and
                data.unique_type):
            events = self._latest_db.query(event_types=data.event_types)

            if data.t_from is not None:
                events = (event for event in events
                          if data.t_from <= event.timestamp)

            events = sorted(events,
                            key=lambda i: (i.timestamp, i.event_id),
                            reverse=True)

            if data.max_results is not None:
                events = events[:data.max_results]

            return events

        subscription = (common.Subscription(data.event_types)
                        if data.event_types is not None else None)

        for db in self._ordered_dbs:
            if db.order_by != data.order_by:
                continue
            if subscription and subscription.isdisjoint(db.subscription):
                continue

            events = await db.query(subscription=subscription,
                                    server_id=data.server_id,
                                    event_ids=data.event_ids,
                                    t_from=data.t_from,
                                    t_to=data.t_to,
                                    source_t_from=data.source_t_from,
                                    source_t_to=data.source_t_to,
                                    payload=data.payload,
                                    order=data.order,
                                    unique_type=data.unique_type,
                                    max_results=data.max_results)
            return list(events)

        return []

    async def query_flushed(self,
                            event_id: common.EventId
                            ) -> typing.AsyncIterable[typing.List[common.Event]]:  # NOQA
        async for events in self._ref_db.query(event_id):
            yield events

    async def flush(self):
        async with self._flush_lock:
            if self._env_closed:
                return

            dbs = [self._sys_db,
                   self._latest_db,
                   *self._ordered_dbs]
            ext_flush_fns = [db.create_ext_flush() for db in dbs]
            events = await self._executor(_ext_flush, self._env, self._ref_db,
                                          ext_flush_fns)
            for session in events:
                self._flushed_events_cbs.notify(session)

    async def _write_loop(self):
        try:
            while True:
                await asyncio.sleep(self._flush_period)
                await aio.uncancellable(self.flush())

        except Exception as e:
            mlog.error('backend write error: %s', e, exc_info=e)

        finally:
            self.close()
            await aio.uncancellable(self._close())

    async def _close(self):
        await self.flush()
        async with self._flush_lock:
            self._env_closed = True
            await self._executor(self._env.close)


def _ext_flush(env, ref_db, flush_fns):
    with env.begin(write=True) as txn:
        ctx = context.Context(txn=txn,
                              ref_db=ref_db)
        for flush_fn in flush_fns:
            flush_fn(ctx)
        return ctx.get_events()
