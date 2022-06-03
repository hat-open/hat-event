from hat import aio
from hat import json
from hat.event.server import common
import hat.monitor.client


async def create_syncer_client(conf: json.Data,
                               backend: common.Backend,
                               monitor_client: hat.monitor.client.Client
                               ) -> 'SyncerClient':
    """Create syncer client

    Args:
        conf: configuration defined by
            ``hat-event://main.yaml#/definitions/syncer_client``
        backend: backend
        monitor_client: monitor client

    """
    cli = SyncerClient()
    return cli


class SyncerClient(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._async_group
