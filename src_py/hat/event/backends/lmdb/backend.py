from collections.abc import Collection
from pathlib import Path
import asyncio
import collections
import contextlib
import logging
import typing

from hat import aio
from hat import json

from hat.event.backends.lmdb import common
from hat.event.backends.lmdb import environment
from hat.event.backends.lmdb import latestdb
from hat.event.backends.lmdb import refdb
from hat.event.backends.lmdb import systemdb
from hat.event.backends.lmdb import timeseriesdb
from hat.event.backends.lmdb.conditions import Conditions


mlog = logging.getLogger(__name__)

cleanup_max_results = 1024

flush_queue_size = 4096

max_registered_count = 1024 * 256

version = '0.9'


class Databases(typing.NamedTuple):
    system: systemdb.SystemDb
    latest: latestdb.LatestDb
    timeseries: timeseriesdb.TimeseriesDb
    ref: refdb.RefDb


class Changes(typing.NamedTuple):
    system: systemdb.Changes
    latest: latestdb.Changes
    timeseries: timeseriesdb.Changes
    ref: refdb.Changes


async def create(conf: json.Data,
                 registered_events_cb: common.BackendRegisteredEventsCb | None,
                 flushed_events_cb: common.BackendFlushedEventsCb | None
                 ) -> 'LmdbBackend':
    backend = LmdbBackend()
    backend._registered_events_cb = registered_events_cb
    backend._flushed_events_cb = flushed_events_cb
    backend._conditions = Conditions(conf['conditions'])
    backend._loop = asyncio.get_running_loop()
    backend._flush_queue = aio.Queue(flush_queue_size)
    backend._registered_count = 0
    backend._registered_queue = collections.deque()
    backend._async_group = aio.Group()

    backend._env = await environment.create(Path(conf['db_path']))
    backend.async_group.spawn(aio.call_on_done, backend._env.wait_closing(),
                              backend.close)

    try:
        latest_subscription = common.create_subscription(
            tuple(i) for i in conf['latest']['subscriptions'])

        timeseries_partitions = (
            timeseriesdb.Partition(
                order_by=common.OrderBy[i['order_by']],
                subscription=common.create_subscription(
                    tuple(event_type) for event_type in i['subscriptions']),
                limit=(
                    timeseriesdb.Limit(
                        min_entries=i['limit'].get('min_entries'),
                        max_entries=i['limit'].get('max_entries'),
                        duration=i['limit'].get('duration'),
                        size=i['limit'].get('size'))
                    if 'limit' in i else None))
            for i in conf['timeseries'])

        backend._dbs = await backend._env.execute(
            _ext_create_dbs, backend._env, conf['identifier'],
            backend._conditions, latest_subscription, timeseries_partitions)

        backend.async_group.spawn(backend._flush_loop, conf['flush_period'])
        backend.async_group.spawn(backend._cleanup_loop,
                                  conf['cleanup_period'])

    except BaseException:
        await aio.uncancellable(backend._env.async_close())
        raise

    return backend


def _ext_create_dbs(env, identifier, conditions, latest_subscription,
                    timeseries_partitions):
    with env.ext_begin(write=True) as txn:
        system_db = systemdb.ext_create(env, txn, version, identifier)
        latest_db = latestdb.ext_create(env, txn, conditions,
                                        latest_subscription)
        timeseries_db = timeseriesdb.ext_create(env, txn, conditions,
                                                timeseries_partitions)
        ref_db = refdb.RefDb(env)

    return Databases(system=system_db,
                     latest=latest_db,
                     timeseries=timeseries_db,
                     ref=ref_db)


class LmdbBackend(common.Backend):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def get_last_event_id(self,
                                server_id: int
                                ) -> common.EventId:
        if not self.is_open:
            raise common.BackendClosedError()

        return self._dbs.system.get_last_event_id(server_id)

    async def register(self,
                       events: Collection[common.Event]
                       ) -> Collection[common.Event] | None:
        if not self.is_open:
            raise common.BackendClosedError()

        for event in events:
            server_id = event.id.server

            last_event_id = self._dbs.system.get_last_event_id(server_id)
            last_timestamp = self._dbs.system.get_last_timestamp(server_id)

            if last_event_id >= event.id:
                mlog.warning("event registration skipped: invalid event id")
                continue

            if last_timestamp > event.timestamp:
                mlog.warning("event registration skipped: invalid timestamp")
                continue

            if not self._conditions.matches(event):
                mlog.warning("event registration skipped: invalid conditions")
                continue

            refs = collections.deque()

            latest_result = self._dbs.latest.add(event)

            if latest_result.added_ref:
                refs.append(latest_result.added_ref)

            if latest_result.removed_ref:
                self._dbs.ref.remove(*latest_result.removed_ref)

            refs.extend(self._dbs.timeseries.add(event))

            if not refs:
                continue

            self._dbs.ref.add(event, refs)
            self._dbs.system.set_last_event_id(event.id)
            self._dbs.system.set_last_timestamp(server_id, event.timestamp)

        self._registered_queue.append(events)
        self._registered_count += len(events)

        if self._registered_count > max_registered_count:
            await self._flush_queue.put(self._loop.create_future())

        if self._registered_events_cb:
            await aio.call(self._registered_events_cb, events)

        return events

    async def query(self,
                    params: common.QueryParams
                    ) -> common.QueryResult:
        if not self.is_open:
            raise common.BackendClosedError()

        if isinstance(params, common.QueryLatestParams):
            return self._dbs.latest.query(params)

        if isinstance(params, common.QueryTimeseriesParams):
            return await self._dbs.timeseries.query(params)

        if isinstance(params, common.QueryServerParams):
            return await self._dbs.ref.query(params)

        raise ValueError('unsupported params type')

    async def flush(self):
        try:
            future = self._loop.create_future()
            await self._flush_queue.put(future)
            await future

        except aio.QueueClosedError:
            raise common.BackendClosedError()

    async def _flush_loop(self, flush_period):
        futures = collections.deque()

        async def cleanup():
            with contextlib.suppress(Exception):
                await self._flush()

            await self._env.async_close()

        try:
            while True:
                try:
                    future = await aio.wait_for(self._flush_queue.get(),
                                                flush_period)
                    futures.append(future)

                except asyncio.TimeoutError:
                    pass

                except aio.CancelledWithResultError as e:
                    if e.result:
                        futures.append(e.result)

                    raise

                while not self._flush_queue.empty():
                    futures.append(self._flush_queue.get_nowait())

                await aio.uncancellable(self._flush())

                while futures:
                    future = futures.popleft()
                    if not future.done():
                        future.set_result(None)

        except Exception as e:
            mlog.error('backend flush error: %s', e, exc_info=e)

        finally:
            self.close()
            self._flush_queue.close()

            while not self._flush_queue.empty():
                futures.append(self._flush_queue.get_nowait())

            for future in futures:
                if not future.done():
                    future.set_exception(common.BackendClosedError())

            await aio.uncancellable(cleanup())

    async def _cleanup_loop(self, cleanup_period):
        try:
            while True:
                await asyncio.sleep(0)

                repeat = await self._env.execute(_ext_cleanup, self._env,
                                                 self._dbs, common.now())
                if repeat:
                    continue

                await asyncio.sleep(cleanup_period)

        except Exception as e:
            mlog.error('backend cleanup error: %s', e, exc_info=e)

        finally:
            self.close()

    async def _flush(self):
        if not self._env.is_open:
            return

        self._registered_count = 0
        registered_queue, self._registered_queue = (self._registered_queue,
                                                    collections.deque())

        changes = Changes(system=self._dbs.system.create_changes(),
                          latest=self._dbs.latest.create_changes(),
                          timeseries=self._dbs.timeseries.create_changes(),
                          ref=self._dbs.ref.create_changes())

        # TODO lock period between create_changes and locking executor
        #      (timeseries and ref must write changes before new queries are
        #      allowed)

        await self._env.execute(_ext_flush, self._env, self._dbs, changes)

        if not self._flushed_events_cb:
            return

        while registered_queue:
            events = registered_queue.popleft()
            await aio.call(self._flushed_events_cb, events)


def _ext_flush(env, dbs, changes):
    with env.ext_begin(write=True) as txn:
        dbs.system.ext_write(txn, changes.system)
        dbs.latest.ext_write(txn, changes.latest)
        dbs.timeseries.ext_write(txn, changes.timeseries)
        dbs.ref.ext_write(txn, changes.ref)


def _ext_cleanup(env, dbs, now):
    with env.ext_begin(write=True) as txn:
        result = dbs.timeseries.ext_cleanup(txn, now, cleanup_max_results)
        if not result:
            return False

        dbs.ref.ext_cleanup(txn, result)

    return len(result) >= cleanup_max_results
