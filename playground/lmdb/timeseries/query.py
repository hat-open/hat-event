from pathlib import Path
import asyncio
import contextlib
import cProfile
import sys
import time

from hat import aio

from hat.event import common
import hat.event.backends.lmdb


def main():
    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main())


async def async_main():
    db_path = Path(sys.argv[1])

    group_id = 0
    max_results = 10_000

    conf = {'db_path': str(db_path),
            'identifier': None,
            'flush_period': 1,
            'cleanup_period': 1,
            'conditions': [],
            'latest': {'subscriptions': []},
            'timeseries': [{'order_by': 'SOURCE_TIMESTAMP',
                            'subscriptions': [['eds', 'timeseries',
                                               str(group_id), '?']]}]}

    backend = await aio.call(hat.event.backends.lmdb.info.create, conf, None,
                             None)

    try:
        t1 = time.monotonic()

        with cProfile.Profile() as pr:
            await backend.query(
                common.QueryTimeseriesParams(
                    event_types=[('eds', 'timeseries', str(group_id), '0')],
                    order_by=common.OrderBy.SOURCE_TIMESTAMP,
                    order=common.Order.DESCENDING,
                    max_results=max_results + 1))

        t2 = time.monotonic()

        print(t2 - t1)

        pr.dump_stats(str(db_path.with_suffix('.profile')))

    finally:
        await aio.uncancellable(backend.async_close())


if __name__ == '__main__':
    main()
