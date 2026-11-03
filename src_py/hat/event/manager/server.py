from collections.abc import Collection

from hat.drivers import net

from hat.event.manager import common


async def run(addr: net.StreamAddress,
              client_name: str,
              client_token: str | None,
              subscriptions: Collection[common.EventType],
              server_id: int | None,
              persisted: bool,
              http_addr: net.StreamAddress):
    raise NotImplementedError()
