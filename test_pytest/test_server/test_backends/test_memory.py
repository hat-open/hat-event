from hat import aio

from hat.event.server import common
import hat.event.server.backends.memory


async def test_create():
    backend = await aio.call(hat.event.server.backends.memory.create, {},
                             None, None)

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
