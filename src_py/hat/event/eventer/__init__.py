"""Eventer communication protocol

This package provides Eventer Client as:
    * low-level interface - `Client`
    * high-level interface - `Component`

`connect` is used for establishing single eventer connection
with Eventer Server which is represented by `Client`. Once
connection is terminated (signaled with `Client.closed`),
it is up to user to repeat `connect` call and create new `Client`
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

`Component` provides high-level interface for continuous communication with
currenty active Event Server based on information obtained from Monitor Server.
This implementation repeatedly tries to create active connection
with Eventer Server. When this connection is created, users code is notified by
calling `component_cb` callback. Once connection is closed, user defined
resource, resulting from `component_cb`, is cancelled and `Component` repeats
connection estabishment process.

"""

from hat.event.eventer.client import (connect,
                                      Client,
                                      Runner,
                                      ComponentCb,
                                      Component)
from hat.event.eventer.server import (ClientId,
                                      ClientCb,
                                      RegisterCb,
                                      QueryCb,
                                      listen,
                                      Server)


__all__ = ['connect',
           'Client',
           'Runner',
           'ComponentCb',
           'Component',
           'ClientId',
           'ClientCb',
           'RegisterCb',
           'QueryCb',
           'listen',
           'Server']
