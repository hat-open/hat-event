from pathlib import Path
import asyncio
import contextlib
import sys

from hat import aio

from hat.event import common
import hat.event.backends.lmdb


def main():
    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main())


async def async_main():
    db_path = Path(sys.argv[1])

    server_id = 1
    group_id = 0
    point_count = 2000
    interval = 5
    retention = 24 * 60 * 60

    conf = {'db_path': str(db_path),
            'identifier': None,
            'flush_period': 1,
            'cleanup_period': 1,
            'conditions': [],
            'latest': {'subscriptions': []},
            'timeseries': [{'order_by': 'SOURCE_TIMESTAMP',
                            'subscriptions': [['eds', 'timeseries',
                                               str(group_id), '?']],
                            'limit': {'duration': retention}}]}

    backend = await aio.call(hat.event.backends.lmdb.info.create, conf, None,
                             None)

    try:
        t = 0
        session_id = 0

        while t < retention:
            session_id += 1
            events = [common.Event(id=common.EventId(server_id, session_id,
                                                     point_id + 1),
                                   type=('eds', 'timeseries', str(group_id),
                                         str(point_id)),
                                   timestamp=common.Timestamp(t, 0),
                                   source_timestamp=common.Timestamp(t, 0),
                                   payload=None)
                      for point_id in range(point_count)]
            await backend.register(events)

            t += interval

    finally:
        await aio.uncancellable(backend.async_close())


if __name__ == '__main__':
    main()
