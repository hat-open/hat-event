import typing

from hat import aio
from hat.event.server import common
import hat.monitor.client


async def create_syncer_client(backend: common.Backend,
                               monitor_client: hat.monitor.client.Client,
                               monitor_group: str,
                               name: str,
                               syncer_token: typing.Optional[str] = None,
                               **kwargs
                               ) -> 'SyncerClient':
    """Create syncer client

    Args:
        backend: backend
        monitor_client: monitor client
        monitor_group: monitor group name
        name: client name
        syncer_token: syncer token
        kwargs: additional arguments passed to `hat.chatter.connect` coroutine

    """
    cli = SyncerClient()
    return cli


class SyncerClient(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._async_group
