.. _event-server:

Event Server
============

Event Server is a central component responsible for registering, processing,
storing and providing access to events.

`Configuration`_ for Event Server is defined with schema
``hat-event://server.yaml#``.


Running
-------

By installing Event Server from `hat-event` package, executable
`hat-event-server` becomes available and can be used for starting this
component.

    .. program-output:: python -m hat.event.server --help

Event Server can be run as standalone component or component connecting
to `Monitor Server`_.


Redundancy
----------

To run multiple redundant Event Servers, `monitor` parameter has to be defined
in the configuration. Event Server connects to configured `Monitor Server`_ and
creates a corresponding Monitor Component. Besides execution of redundancy
algorithm, Monitor Server is also used for service discovery. Each Monitor
Component is described with its `info`, that is `Component Information`_. One
of the properties of component `info` is `data`, through which a component can
share some arbitrary information with any other component. Event Server uses
`data` (specified by ``hat-event://monitor_data.yaml#``) to share the following
information:

.. _monitor_component_data:

    * `eventer_server_address`

        Eventer Server address is needed to any component that uses Eventer
        Client to interact with currently active Event Server by events.

    * `syncer_server_address`

        Syncer Server address is needed to any component that uses Syncer Client
        to synchronize with currently active Event Server.

    * `server_id`

        Event Server unique identifier (defined in configuration as
        `server_id`). Needed to Syncer Client in order to request
        synchronization with the specific server.

    * `syncer_token` (optional property)

        This token string is used as a key for synchronization between servers.
        Event Server will request synchronization with a remote Event Server
        only if their `syncer_token` are identical (for more details see
        `Syncer Client`_). If this token is not defined, synchronization is
        performed irrespective.


Components
----------

Event Server functionality can be defined by using the following components:

.. uml::

    folder "Hat Component" <<Component>> {
        component "Eventer Client" as EventerClient
    }

    folder "External Application" <<Component>> {
        component "Mariner Client" as MarinerClient
    }

    folder "Event Server" {
        component "Eventer Server" as EventerServer
        component "Engine" as Engine
        component "Module" <<Module>> as Module
        component "Backend" as Backend
        component "Syncer Server" as SyncerServer
        component "Syncer Client" as SyncerClient
        component "Mariner Server" as MarinerServer

        interface subscribe
        interface notify
        interface register
        interface query
    }

    folder "Remote Event Server" {
        component "Syncer Server" as RemoteSyncerServer
        component "Syncer Client" as RemoteSyncerClient
        component "Backend" as RemoteBackend
    }

    database "Database" as Database
    database "Database" as RemoteDatabase

    EventerServer -- subscribe
    EventerServer -- notify
    EventerServer -- register
    EventerServer -- query

    subscribe <-- EventerClient
    notify --> EventerClient
    register <-- EventerClient
    query <-- EventerClient

    Engine <-> EventerServer
    Engine --> Backend

    Module --o Engine

    Backend --> Database

    SyncerServer <-- Backend
    SyncerClient --> Backend
    SyncerServer --> Engine

    RemoteSyncerServer <-- RemoteBackend
    RemoteSyncerClient --> RemoteBackend
    RemoteBackend --> RemoteDatabase

    RemoteSyncerClient -> SyncerServer
    SyncerClient -> RemoteSyncerServer

    MarinerClient <-- MarinerServer
    MarinerServer <-- Backend
    MarinerServer --> Engine


Eventer Server
''''''''''''''

Eventer Server module is responsible for providing implementation of server side
of :ref:`Eventer communication <eventer>`. This component translates client
requests to Engine's method calls. At the same time, it observes all new event
notifications made by `Engine`_ and notifies clients with appropriate messages.

`RegisterEvent` objects obtained from client's register requests are forwarded
to Engine which converts them to `Event` and submits to further processing.

A unique identifier is assigned to each chatter connection established with
Eventer Server (unique for the single execution lifetime of Event Server
process). This identifier is associated with all `Event`s obtained from the
corresponding connection as through `id` of `Source` (source `type` is
``EVENTER``).

Each Client makes its subscription to Eventer Server by its last subscription
message `MsgSubscribe`. This subscription is used as a filter for selecting
subset of event notifications which are sent to associated connection.

Eventer Server module is responsible for registering events each time new
chatter connection is established and each time existing chatter connection is
closed. These events are defined by unique `event type`:

    * 'event', 'eventer'

        * `source_timestamp` is ``None``

        * `payload` is specified by
          ``hat-event://server.yaml#/definitions/events/eventer``.

This events are registered through `Engine`_ with `Source.type` ``EVENTER``.


Syncer Client
'''''''''''''

Syncer Client module is responsible for providing implementation of client side
of :ref:`Syncer communication <syncer>`. It is instanced with reference to
`Backends`_ instance and a Monitor Client instance from (see `Monitor Server`_).

Event Server uses Syncer Client to synchronize with a remote Event Server. In
case when Event Server is run without Monitor Server (without `monitor` in
configuration), Syncer Client is not run: no other Event Servers are expected,
so there is no need for synchronization.

Syncer Client uses Monitor Client in order to discover remote Event Servers,
that is their Syncer Servers. It registers for notifications made by Monitor
Client once global state changes. Once it gets `ComponentInfo`, with same
`group` as its own (correspond to remote Event Server), it is interested in
its `data` property in order to get `syncer_token`, `server_id` and
`syncer_server_address` (see :ref:`here <monitor_component_data>`). First it
checks that `syncer_token` of the remote server matches with its own
`syncer_token` (defined in configuration). This mechanism can be used, for
example, to check if remote server node has matching system configuration. If
they doesn't match, client ignores that component and doesn't connect to its
Syncer Server. Otherwise, client uses `syncer_server_address` to connect to the
remote Syncer Server. Once it connects, client immediately sends `MsgReq` in
order to request synchronization. The main reference for synchronization of
events is `lastEventId`, that it gets from backend using `server_id` obtained
from Monitor Client.

On all the `MsgEvents` messages received from the Syncer Server, client
transforms messages to corresponding Events and registers them to backend
(see :ref:`Syncer communication <syncer>` for more details about messages
exchanged with server).

In case client discovered (through Monitor Client) multiple Syncer Servers, it
will connect to all of them and try to synchronize with all of them
independently.

Syncer Client is responsible for registering events in regard to
synchronization status of all active Syncer Server connections. This event is
defined with unique event type:

    * 'event', 'syncer', 'client', <server_id>, 'synced'

        * `source_timestamp` is ``None``

        * `payload` is specified by
          ``hat-event://server.yaml#/definitions/events/syncer/client``.

More precisely, this event is not registered by the Syncer Client itself, but
Event Server registers it on behalf of Syncer Client, with `Source.type` set to
``SYNCER`` (for the sake of simplicity hereafter we consider as Syncer Client
registers this event).

Payload of this event is boolean flag that represents current synchronization
state. When client receives remote events prior to receiving `MsgSynced`
message (see :ref:`Syncer communication <syncer>`), it will register new
`synced` event with payload ``False``. When client receives `MsgSynced`, it will
register new `synced` event with payload ``True``. If no new events are received
prior to receiving `MsgSynced`, `synced` ``True`` event is registered without
prior registration of associated `synced` ``False`` event.


Syncer Server
'''''''''''''

Syncer Server module is responsible for providing implementation of server side
of :ref:`Syncer communication <syncer>`. It is in charge of synchronizing
arbitrary number of clients connected to it, with all the events from its Event
Server.

Server is instanced with backend instance. It registers to backend notifications
of newly stored events. Server also retrieves events from backend, needed for a
client synchronization, by its query method `query_from_event_id`, specialized
for Syncer Server usage.

Syncer Server is responsible for registering event in regard to connection
status of all active Syncer Client. This event represents current Syncer Server
state and should be registered immediately after Engine initialization and each
time this state changes. Event is defined with unique event type:

    * 'event', 'syncer', 'server'

        * `source_timestamp` is ``None``

        * `payload` is specified by
          ``hat-event://server.yaml#/definitions/events/syncer/server``.

More precisely, this event is not registered by the Syncer Server itself, but
Event Server registers it on behalf of Syncer Server, with `Source.type` set to
``SYNCER`` (for the sake of simplicity hereafter we consider as Syncer Server
registers this event).

Payload of this event contains list of all currently active client connections.
Once connection between server and client is broken, it will no longer be
part of future event's payloads. New entry to list of active connections is
added once server receives `MsgReq` from client. Each client connection is
represented with:

    * `client_name`

        Name of the Syncer Client defined as `clientName` in `MsgReq` message
        (see :ref:`Syncer communication <syncer>`)

    * `synced`

        Status flag which is initially set to ``False``. Once server has sent
        all the events that client requested to get "synchronized", it changes
        connection's `synced` status to ``True``.

Multiple clients can be connected to a server, where each connection is handled
independently, as described.


Mariner Server
''''''''''''''

...


Engine
''''''

Engine is responsible for creating `Modules`_ and coordinating event
registration, executing processing sessions and backend (see `Backends`_).

Engine's method `register` enables registration of events to backend based on a
list of register events. Entity who requests registration is identified by
`Source` identifier which is used only during processing of events and is
discarded once event is created. There are three types of sources that may
register events on Engine:

    - ``SYNCER``: `Syncer Server`_
    - ``EVENTER``: `Eventer Server`_
    - ``ENGINE``: `Engine`
    - ``MODULE``: any of the `Modules`_

By creating event, Engine enhances register event with

    - `event_id`: unique event identifier,
    - `timestamp`: single point in time when events are registered. All events
      registered in a single session get the same `timestamp`.

Process of creating events based on a single list of register events provided
through `register` is called session. Engine starts new session each time
Eventer Server, Syncer Server or a module requests new registration through
`register` method. After processing session ends, events are registered with
backend. Start and end of each session is notified to each module by
calling module's `on_session_start` and `on_session_stop` methods respectively.
These methods are called sequentially for each module: only when method of one
module ended, method of the other module can be called.

During session processing, each module is notified with a newly created event in
case it matches its subscription. By subscription, each module defines event
type filter condition used for filtering new events that will get notified to
the module. Processing of this event by module can result with new register
events. Out of register events, Engine creates events and adds them to the
current session. All modules, including the one that added that new events, are
notified with new additions. This process continues iteratively until all
modules return empty lists of new register events. Processing events by single
module is always sequential - Engine keeps order of new events added to session
so that new events are always added to the end of the queue for module
notification.

.. warning ::

    Care should be taken by module implementation not to cause self recursive or
    mutually recursive endless processing loop.

Engine registers an event that signalizes when the Engine was started or
stopped. Event is registered by Engine, with `Source.Type` set to
`ENGINE` and with the following event type::

    * 'event', 'engine'

        * `source timestamp` - None

        * `payload`  is specified by
          ``hat-event://server.yaml#/definitions/events/engine``.


Modules
'''''''

.. warning::

    Event Server does not provide sandbox environment for loading end executing
    modules. Modules have full access to Event Server functionality which is
    controlled with module execution. Module implementation and configuration
    should be written in accordance to other modules and Event Server as a
    whole, keeping in mind processing execution time overhead and possible
    interference between modules.

Each module represents predefined and configurable closely related functions
that can modify the process of registering new events or initiate new event
registration sessions. When created, module is provided with reference to
`Engine`_ which can be used for registering and querying events and its own
unique source identification (used in case of registering new events).

Implementation of specific module can be maintained independently of Event
Server implementation. Location of Python module implementing Event Server's
module is defined by Event Server's configuration.


Backends
''''''''

Backend is wrapper for storing and retrieving events from specialized storage
engines.

Backends available as part of `hat-event` package:

    .. toctree::
       :maxdepth: 1

       backends/dummy
       backends/lmdb

Other backend implementations can be maintained independently of Event
Server implementation. Location of Python module implementing backend is
defined by Event Server's configuration.


Implementation
--------------

Documentation is available as part of generated API reference:

    * `Python hat.event.server module <py_api/hat/event/server.html>`_


Configuration
-------------

.. literalinclude:: ../schemas_json/server.yaml
    :language: yaml


.. |arr| unicode:: U+003E
.. _Monitor Server: https://hat-monitor.hat-open.com/monitor.html
.. _Component Information: https://hat-monitor.hat-open.com/monitor.html#component-information
