from hat import aio

from hat.event import common
from hat.event.backends.memory import info


async def test_create():
    backend = await aio.call(info.create, {}, None, None)

    assert isinstance(backend, common.Backend)
    assert backend.is_open

    await backend.async_close()
    assert backend.is_closed


async def test_get_last_event_id():
    # TODO
    pass


async def test_register():
    # TODO
    pass


async def test_query():
    # TODO
    pass
