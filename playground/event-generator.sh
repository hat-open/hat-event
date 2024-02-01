#!/bin/sh

set -e

PLAYGROUND_PATH=$(dirname "$(realpath "$0")")
. $PLAYGROUND_PATH/env.sh

exec $PYTHON << EOF
import asyncio
import contextlib

from hat import aio
from hat.drivers import tcp

from hat.event import common
import hat.event.eventer

def main():
    aio.init_asyncio()
    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main())

async def async_main():
    addr = tcp.Address('127.0.0.1', 23012)
    conn = await hat.event.eventer.connect(addr, 'client')
    try:
        register_event = common.RegisterEvent(
            type=('a', 'b', 'c'),
            source_timestamp=common.now(),
            payload=common.EventPayloadJson(123))
        result = await conn.register([register_event], with_response=True)
        print(result)
    finally:
        await conn.async_close()

main()
EOF
