.. _server:

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

    * `server_id`

        Event Server unique identifier (defined in configuration as
        `server_id`). Needed by Eventer Client to request synchronization with
        the specific server.

    * `eventer_server/host` and `eventer_server/host`

        Eventer Server address is needed to any component that uses Eventer
        Client to interact with currently active Event Server by events.

    * `syncer_token` (optional property)

        This token string is used as a key for synchronization between Eventer
        Server and Eventer Client. Event Server will request synchronization
        with a remote Event Server only if their `server_token` are identical.
        If this token is not defined, synchronization is performed
        irrespective.

Synchronization between Event Servers is based on
:ref:` Eventer communication protocol <eventer>`.

If redundant execution is used, each Event Server can operate in two distinct
modes:

* `standby`
* `operating`


Components
----------

Event Server functionality can be defined by using the following components:

.. uml::

    folder "Hat Component" <<Component>> {
        component "Eventer Client" as ComponentEventerClient
    }


    folder "Event Server" {
        component "Eventer Server" as EventerServer
        component "Eventer Client" as EventerClient
        component "Engine" as Engine
        component "Module" <<Module>> as Module
        component "Backend" as Backend

        interface subscribe
        interface notify
        interface register
        interface query
    }

    folder "Remote Event Server" {
        component "Eventer Server" as RemoteEventerServer
        component "Eventer Client" as RemoteEventerClient
        component "Backend" as RemoteBackend
    }

    database "Database" as Database
    database "Database" as RemoteDatabase

    EventerServer -- subscribe
    EventerServer -- notify
    EventerServer -- register
    EventerServer -- query

    subscribe <-- ComponentEventerClient
    notify --> ComponentEventerClient
    register <-- ComponentEventerClient
    query <-- ComponentEventerClient

    EventerServer --> Backend
    EventerServer -> Engine
    EventerClient --> Backend
    Engine --> Backend

    Module --o Engine

    Backend --> Database

    RemoteEventerServer --> RemoteBackend
    RemoteBackend --> RemoteDatabase

    RemoteEventerClient -> EventerServer
    EventerClient -> RemoteEventerServer


.. _server-engine:

Engine
''''''

Engine is responsible for creating :ref:`Modules <modules>` and executing
processing sessions.

This module is available only during `operational` mode.

Engine's method `register` enables registration of events to backend based on
a list of register events. Entity who requests registration is identified by
`Source` identifier which is used only during processing of events and is
discarded once event is created. There are three types of sources that may
register events on Engine:

* ``EVENTER``
  Events register by `Eventer Server`_ (originating from other components).

* ``MODULE``
  Events registered by :ref:`Modules <modules>`.

* ``ENGINE``
  Events registered by engine itself (e.g. starting and stopping of engine).

* ``SERVER``
  Events registered by Event Server internal functions (e.g. synchronization
  status).

By creating event, Engine enhances register event with:

* `event_id` - unique event identifier,
* `timestamp` - single point in time when events are registered. All events
  registered in a single session get the same `timestamp`.

Process of creating events based on a single list of register events provided
through `register` is called session. Engine starts new session each time
Eventer Server or a module requests new registration through `register` method.
After processing session ends, events are registered with backend. Start and
end of each session is notified to each module by calling module's
`on_session_start` and `on_session_stop` methods respectively.
These methods are called sequentially for each module: only when method of one
module ended, method of the other module can be called.

During session processing, each module is notified with a newly created event
in case it matches its subscription. By subscription, each module defines event
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

    Care should be taken by module implementation not to cause self recursive
    or mutually recursive endless processing loop.

Engine registers an event that signalizes when the Engine was started or
stopped. Event is registered by Engine, with `Source.Type` set to
`ENGINE` and with event type ``event/engine``. Source timestamp is set to
``None`` and payload is specified by
``hat-event://events.yaml#/definitions/events/engine``.


Eventer Server
''''''''''''''

Eventer Server module is responsible for providing implementation of server side
:ref:`Eventer communication <eventer>`. This component translates client
registration requests to `Engine`_ `register` method calls and client
queries to :ref:`Backend <backends>` `query` method calls. At the same time, it
observes all new event notifications made by Backend and notifies clients with
appropriate messages.

This module is available during `standby` and `operational` modes
(event registration is not possible in `standby` mode).

`RegisterEvent` objects obtained from client's register requests are forwarded
to Engine which converts them to `Event` and submits to further processing.

A unique identifier is assigned to each chatter connection established with
Eventer Server (unique for the single execution lifetime of Event Server
process). This identifier is associated with all `Event`s obtained from the
corresponding connection as through `id` of `Source` (source `type` is
``EVENTER``).

Each Client makes its subscription to Eventer Server during connection
initialization. This subscription is used as a filter for selecting
subset of event notifications which are sent to associated connection.
In addition to subscription, client can request to be notified only with
events with specific `server_id`.

Eventer Server module is responsible for registering events each time new
chatter connection is established and each time existing chatter connection is
closed. These events are defined by event type ``event/eventer/<client_name>``
where ``<client_name>`` is client identifier obtained during connection
initialization. For these events, source timestamp is set to ``None`` and
payload is specified by
``hat-event://events.yaml#/definitions/events/eventer``.


Eventer Client
''''''''''''''

If multiple redundant Event Servers are used, each Event Server runs
Eventer Client module. This module is used for obtaining and active
synchronization of events originating from other Event Server instances.

This module is available during `standby` and `operational` modes.

By continuously monitoring state reported by `Monitor Server`_, Event Server
keeps updated list of other available Event Server instances. For each other
available instance, new Eventer Client is created. Immediately after
connecting to remote Event Server, Eventer Client will query all server events
following last event available in local backend. When initial events are
retrieved, Eventer Client keeps active connection to remote Event Server and
registers new events as they are received. Eventer Client registers received
events directly to backend overriding Engines event processing.

During communication with remote Event Server, if Event Server is in
operational mode, additional events representing synchronization status will be
registered. These events are registered with Engine's `register` method
together with source ``SERVER``. Event type is ``event/synced/<server_id>``
where ``<server_id>`` represent identification of remote server. Source
timestamp is set to ``None`` and payload is specified by
``hat-event://events.yaml#/definitions/events/synced``. Immediatly after
connection with remote Event Server is established, synced event with payload
``False`` is registered. Once all remote events are queried, new synced event
with payload ``True`` is registered.

Based on configuration property ``synced_restart_engine``, Engine can be
restarted if synchronization of more than ``0`` events occur.


Extensions
----------

Core Event Server functionality is used as basis which is extended with:

.. toctree::
    :maxdepth: 1

    modules/index
    backends/index


Implementation
--------------

Documentation is available as part of generated API reference:

    * `Python hat.event.server module <py_api/hat/event/server.html>`_


Configuration
-------------

.. literalinclude:: ../../schemas_json/server.yaml
    :language: yaml


.. |arr| unicode:: U+003E
.. _Monitor Server: https://hat-monitor.hat-open.com/monitor.html
.. _Component Information: https://hat-monitor.hat-open.com/monitor.html#component-information
