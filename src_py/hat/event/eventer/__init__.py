"""Eventer communication protocol

This package provides eventer client as:
    * low-level interface - `connect`/`EventerClient`
    * high-level interface - `run_client`

`connect` is used for establishing single eventer connection
with Eventer Server which is represented by `EventerClient`. Once
connection is terminated (signaled with `EventerClient.closed`),
it is up to user to repeat `connect` call and create new `EventerClient`
instance, if additional communication with Event Server is required.

Example of low-level interface usage::

    client = await hat.event.eventer.connect(
        'tcp+sbs://127.0.0.1:23012',
        [['x', 'y', 'z']])

    registered_events = await client.register_with_response([
        hat.event.common.RegisterEvent(
            event_type=['x', 'y', 'z'],
            source_timestamp=hat.event.common.now(),
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.BINARY,
                data=b'test'))])

    received_events = await client.receive()

    queried_events = await client.query(
        hat.event.common.QueryData(
            event_types=[['x', 'y', 'z']],
            max_results=1))

    assert registered_events == received_events
    assert received_events == queried_events

    await client.async_close()

`run_client` provides high-level interface for continuous
communication with currenty active Event Server based on information obtained
from Monitor Server. This function repeatedly tries to create active connection
with Eventer Server. When this connection is created, users code is notified by
calling `run_cb` callback. Once connection is closed, execution of
`run_cb` is cancelled and `run_client` repeats connection
estabishment process.

Example of high-level interface usage::

    async def monitor_run(component):
        await hat.event.eventer.run_client(
            monitor_client=monitor,
            server_group='event servers',
            run_cb=event_run])

    async def event_run(client):
        while True:
            assert not client.is_closed
            await asyncio.sleep(10)

    monitor = await hat.monitor.client.connect({
        'name': 'client',
        'group': 'test clients',
        'monitor_address': 'tcp+sbs://127.0.0.1:23010'})
    component = hat.monitor.client.Component(monitor, monitor_run)
    component.set_enabled(True)
    await monitor.async_close()

"""

from hat.event.eventer.client import (RunClientCb,
                                      connect,
                                      Client,
                                      run_client)
from hat.event.eventer.server import (ClientId,
                                      ClientCb,
                                      RegisterCb,
                                      QueryCb,
                                      listen,
                                      Server)


__all__ = ['RunClientCb',
           'connect',
           'Client',
           'run_client',
           'ClientId',
           'ClientCb',
           'RegisterCb',
           'QueryCb',
           'listen',
           'Server']
