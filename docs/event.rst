Event Server
============

Event Server is a central component responsible for registering, processing,
storing and providing access to events.

`Configuration`_ for Event Server is defined with schema
``hat-event://main.yaml#``.


Running
-------

By installing Event Server from `hat-event` package, executable `hat-event`
becomes available and can be used for starting this component.

    .. program-output:: python -m hat.event.server --help

Event Server can be run as standalone component or component connecting
to `Monitor Server`_.


Event
-----

Event is a generic data structure used for communication and storage of relevant
changes happening through time in a Hat system. For example, event can
represent a data change triggered by a device outside of the system, or an
inner notification created by a component inside the system. Each event is
immutable and uniquely identified by its event id. Event Server is the only
component responsible for creating events - all other components shall request
Event Server to create a new event.

Event data structure:

    * `id`

        A unique Event identifier always set by the server. It contains:

            * `server`

                Event Server unique identifier (defined in configuration as
                `server_id`).

            * `session`

                Unique identifier of event processing session (unique only
                on the server that creates it). It is sequential integer that
                starts from ``1``. Session of value ``0`` is never assigned to
                an event - it is reserved for identifier of non-existing session
                preceding first available session.

            * `instance`

                Unique identifier of an Event instance (unique only
                for the session and server that created it). It is sequential
                integer that starts from ``1``. Instance of value 0 is never
                assigned to an event, it is reserved for identifier of
                non-existing instance preceding first available instance in
                associated session.

    * `type`

        Type is a user (client) defined list of strings. Semantics of the list's
        elements and their position in the list is determined by the user and
        is not predefined by the Event Server. This list should be used as the
        main identifier of the occurred event's type. Each component
        registering an event should have its own naming convention defined
        which does not collide with other components' naming conventions. This
        property is set by the user while registering an event. Subtypes ``?``
        and ``*`` are not allowed as parts of event type.

        When used in querying and subscription, this property has additional
        semantics. Any string in the list can be replaced with ``?`` while the
        last string can also be replaced with ``*``. Replacements must
        substitute an entire string in the list. The semantics of these
        replacements are:

            * The string ``?``

                is matched with a single arbitrary string.

            * The string ``*``

                is matched with any number (zero or more) of arbitrary strings.

    * `timestamp`

        This property determines the moment when the event was registered on the
        server. It is always set by the server.

    * `source timestamp`

        This property is optional. It represents the moment the Event occurred
        as detected by the source of the Event. It is always set by the
        `Eventer Client`_ (in the remainder of this article mostly referenced
        only as `client`).

    * payload

        This property is optional. It can be used to provide additional data
        bound to an event. The payload property is always set by the client
        registering the event. Its contents are determined by the Event's
        `type` and can be decoded by clients who understand Events of that
        `type`. Payload can be encoded as binary data, JSON data or SBS
        encoded data. Event Server doesn't decode payload while receiving
        event requests, storing events or providing query results. Payload
        can be optionally decoded by Event Server's modules.


Eventer communication
---------------------

Communication between `Eventer Server`_ and `Eventer Client`_ (in the remainder
of this article, mostly referred only as `server` and `client`) is based on
`Chatter`_ communication, defined in `HatEventer` SBS module (see
`Eventer chatter messages`_) with the following messages:

    +--------------------+----------------------+-----------+
    |                    | Conversation         |           |
    | Message            +-------+------+-------+ Direction |
    |                    | First | Last | Token |           |
    +====================+=======+======+=======+===========+
    | MsgSubscribe       | T     | T    | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+
    | MsgNotify          | T     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgRegisterReq     | T     | T/F  | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+
    | MsgRegisterRes     | F     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgQueryReq        | T     | F    | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+
    | MsgQueryRes        | F     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+

Actions available to clients which are directly mapped to exchange of
communication messages:

    * subscribe

        A client can, at any time, subscribe to Events of certain types by
        sending a subscribe message (`MsgSubscribe`) to the server. After
        server receives subscribe message, it will spontaneously notify the
        client (by sending `MsgNotify`) whenever an Event occurs with the
        `type` matched to any `type` in the subscription message. Matching
        is done as described in event's type property including the ``?`` and
        ``*`` options. A client can send as many subscribe messages as it
        wants, each new subscription message implicitly invalidates previous
        subscriptions. Initially, after new connection between server and
        client is established, client isn't subscribed to any events. Both
        subscribe and notify messages can be sent at any time independently
        of other communication messages. Events that are notified by single
        `MsgNotify` are part of same event processing session.

    * register event

        A client can, at any time, send new request for event registration.
        Those register requests are sent as part of `MsgRegisterReq` message.
        Single `MsgRegisterReq` may contain an arbitrary number of registration
        requests which are all registered at the same time (as part of single
        processing session). Single register event contains event type; and
        optional source timestamp and payload. Upon receiving `MsgRegisterReq`,
        it is responsibility of a server to create new event for each register
        event. All events created based on a single `MsgRegisterReq` have the
        same timestamp and same session identifier. If a client doesn't
        end chatter conversation (`MsgRegisterReq` last flag is ``false``),
        once associated events are created, server will respond with
        `MsgRegisterRes` and end conversation. For each register event in
        `MsgRegisterReq`, associated `MsgRegisterRes` contains newly created
        event, or information about event registration failure.

    * query events

        At any time, client can initiate new event query by sending
        `MsgQueryReq` message. Upon receiving query request, server will
        provide all available events that match query criteria as part
        of single `MsgQueryRes`. Single query request can contain multiple
        filter conditions which ALL must be met for all events provided to
        client as query result. Query request contains:

        * ids - optional filter condition

            If set, only events with ids which are defined as part of filter
            condition are matched.

        * types - optional filter condition

            List of event types. If set, event type has to match at least one
            type from the list. Matching is done as defined in event's **type**
            property description - including the ``?`` and ``*`` options.

        * from timestamp - optional filter condition

            If set, only events with `timestamp` greater than or equal are
            matched.

        * to timestamp - optional filter condition

            If set, only events with **timestamp** lower than or equal are
            matched.

        * from source timestamp - optional filter condition

            If set, only events with `source timestamp` defined, and greater
            than or equal, are matched.

        * to source timestamp - optional filter condition

            If set, only events with `source timestamp` defined, and lower
            than or equal, are matched.

        * payload - optional filter condition

            If set, only events with `payload` defined and whose `payload`
            is the same as the query's `payload` are matched.

        * order

            Can be set to ``ascending`` or ``descending``. If set to
            ``ascending``, matched Events will be returned ordered from the
            earliest to the latest dependent on their `timestamp` or
            `source timestamp` (this choice is determined by the `order by`
            property of the query). Earliest meaning lower timestamp, latest
            meaning greater timestamp. If set to descending the same logic
            applies, but the order is reversed.

        * order by

            Can be set to ``timestamp`` or ``source timestamp``. Ordering Events
            by ``source timestamp`` has events with `source timestamp` undefined
            returned last in an arbitrary order.

        * unique type

            If set to ``true``, it determines whether the matched Events will
            contain only one event instance of the same type. With the query
            `order` set to ``descending``, only one Event with the greatest
            `timestamp` or `source timestamp` will be matched. Setting the
            `order` to ``ascending`` will match the Event with the lowest
            `timestamp` or `source timestamp`.

        * max results

            If set, limits the number of matched Events to this number. Matched
            Events are dependent on the query `order` the same way as in
            `unique type`.


Redundancy
----------

To run multiple redundant Event Servers, `monitor` parameter has to be defined
in the configuration. Event Server connects to configured `Monitor Server`_ and
creates a corresponding Monitor Component. Besides execution of redundancy
algorithm, Monitor Server is also used for service discovery. Each Monitor
Component is described with its `info`, that is `Component Information`_. One
of the properties of component `info` is `data`, through which a component can
share some arbitrary information with any other component. Event server uses
`data` (specified by ``hat-event://monitor_data.yaml#``) to share the following information:

.. _monitor_component_data:

    * `eventer_server_address`

        Eventer server address is needed to any component that uses eventer
        client to interact with currently active Event Server by Events.

    * `syncer_server_address`

        Syncer server address is needed to any component that uses syncer client
        to synchronize with currently active Event Server.

    * `server_id`

        Event server unique identifier (defined in configuration as
        `server_id`). Needed to syncer client in order to request
        synchronization with the specific server.

    * `syncer_token` (optional property)

        This token string is used as a key for synchronization between servers.
        Event server will request synchronization with a remote Event server
        only if their `syncer_token` are identical (for more details see
        `Syncer Client`_). If this token is not defined, synchronization is
        performed irrespective.


Syncer communication
--------------------

Communication between `Syncer Server`_ and `Syncer Client`_, based on chatter
communication, is aimed primarily for events synchronization between two, or
more redundant Event Servers. Thus, each Event server implements both server
and client side of this communication. Generally, this communication is made
not to be exclusively used within an Event Server: syncer client can be used to
establish synchronization of any remote instance (e.g. an arbitrary database)
with an Event Server.

Messages used in Syncer communication, defined in `HatSyncer` SBS
module (see `Syncer chatter messages`_), are the following:

    +--------------------+----------------------+-----------+
    |                    | Conversation         |           |
    | Message            +-------+------+-------+ Direction |
    |                    | First | Last | Token |           |
    +====================+=======+======+=======+===========+
    | MsgReq             | T     | T    | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+
    | MsgSynced          | T     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgEvents          | T     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+

Communication between syncer client and server starts with synchronization
request initiated by the client with `MsgReq` message. The connection between
server and client is considered established when `MsgReq` message is received
on the server side. With this message, client requests for server events in
order to get "synchronized". More specifically, by `MsgReq` client represents
itself by `clientName` and requests all the events that have `EventId` such
that `session` is same or greater and `instance` is greater (ie. newer) than
`lastEventId` and `server` corresponds to syncer server's Event Server.

Upon receiving `MsgReq`, server communicates back the requested events by
`MsgEvents` message. Events are grouped in `MsgEvents` such that all events in
the message are from the same session (`session` of `EventId`). Once all of
these events are sent, server sends `MsgSynced` message in order to signalize
the client that synchronization phase ended.

All events created (by the Event Server with running syncer server) after the
`MsgReq` (and before `MsgSynced`) are not included in the synchronization phase.
They are buffered on the server side and sent to client only after the
`MsgSynced` message, using the same `MsgEvents` message. In the remaining life
of the connection between syncer server and client, only `MsgEvents` messages
are sent from server to a client in order to notify client with the new Events.
All events in one `MsgEvents` message always belong to the same session.

.. todo::

    add `subscription` to `MsgReq`.


Components
----------

Event Server functionality can be defined by using the following components:

.. uml::

    folder "Component 1" <<Component>> {
        component "Eventer Client" as EventerClient1
    }

    folder "Component 2" <<Component>> {
        component " Eventer Client" as EventerClient2
    }

    folder "Event Server" {
        component "Eventer Server" as EventerServer
        component "Engine" as Engine
        component "Module 1" <<Module>> as Module1
        component "Module 2" <<Module>> as Module2
        component "Module 3" <<Module>> as Module3
        component "Backend" as Backend
        component "Syncer server" as SyncerServer
        component "Syncer client" as SyncerClient

        interface subscribe
        interface notify
        interface register
        interface query
    }

    folder "Remote Event Server" {
        component "Syncer server" as RemoteSyncerServer
        component "Syncer client" as RemoteSyncerClient
        component "Backend" as RemoteBackend
    }

    database "Database" as Database
    database "Database" as RemoteDatabase

    EventerServer -- subscribe
    EventerServer -- notify
    EventerServer -- register
    EventerServer -- query

    subscribe <-- EventerClient1
    notify --> EventerClient1
    register <-- EventerClient1
    query <-- EventerClient1

    subscribe <-- EventerClient2
    notify --> EventerClient2
    register <-- EventerClient2
    query <-- EventerClient2

    Engine <-> EventerServer
    Engine --> Backend

    Module1 --o Engine
    Module2 --o Engine
    Module3 --o Engine

    Backend --> Database

    SyncerServer <-- Backend
    SyncerClient --> Backend
    SyncerServer --> Engine

    RemoteSyncerServer <-- RemoteBackend
    RemoteSyncerClient --> RemoteBackend
    RemoteBackend --> RemoteDatabase

    RemoteSyncerClient --> SyncerServer
    SyncerClient --> RemoteSyncerServer


Eventer Client
''''''''''''''

Eventer Client is a component that provides client functionality in
`Eventer communication`_. Package `hat-event` provides python implementation of
`hat.event.eventer_client` module which can be used as a basis for
communication with Eventer Server. This module provides low-level and
high-level communication API. For more detail see documentation of
`hat.event.eventer_client` module.


Eventer Server
''''''''''''''

Eventer Server module is responsible for providing implementation of server side
of `Eventer communication`_. This component translates client requests to
engine's method calls. At the same time, it observes all new event
notifications made by `Engine`_ and notifies clients with appropriate messages.

`RegisterEvent` objects obtained from client's register requests are forwarded
to engine which converts them to `Event` and submits to further processing.

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
          ``hat-event://main.yaml#/definitions/events/eventer``.

This events ar registered through `Engine`_ with `Source.type` ``EVENTER``.


Syncer Client
'''''''''''''

Syncer Client module is responsible for providing implementation of client side
of `Syncer communication`_. It is instanced with reference to `Backend`_
instance and a monitor client instance from (see `Monitor Server`_).

Event Server uses syncer client to synchronize with a remote Event Server. In
case when Event Server is run without Monitor Server (without `monitor` in
configuration), syncer client is not run: no other Event Servers are expected,
so there is no need for synchronization.

Syncer Client uses Monitor Client in order to discover remote Event Servers,
that is their syncer servers. It registers for notifications made by Monitor
Client once global state changes. Once it gets `ComponentInfo`, with same
`group` as it's own (correspond to remote Event Server), it is interested in
its `data` property in order to get `syncer_token`, `server_id` and `syncer_server_address` (see :ref:`here <monitor_component_data>`). First it
checks that `syncer_token` of the remote server matches with its own
`syncer_token` (defined in configuration). This mechanism can be used, for
example, to check if remote server node has matching system configuration. If
they doesn't match, client ignores that component and doesn't connect to its
syncer server. Otherwise, client uses `syncer_server_address` to connect to the
remote syncer server. Once it connects, client immediately sends `MsgReq` in
order to request synchronization. The main reference for synchronization of
events is `lastEventId`, that it gets from Backend using `server_id` obtained
from Monitor Client.

On all the `MsgEvents` messages received from the syncer server, client
transforms messages to corresponding Events and registers them to Backend
(see `Syncer communication`_ for more details about messages exchanged with
server).

In case client discovered (through Monitor Client) multiple syncer servers, it
will connect to all of them and try to synchronize with all of them
independently.


Syncer Server
'''''''''''''

Syncer Server module is responsible for providing implementation of server side
of `Syncer communication`_. It is in charge of synchronizing arbitrary number
of clients connected to it, with all the events from its Event Server they are
subscribed to.

Server is instanced with Backend instance. It registers to Backend notifications
of newly stored events. Server also retrieves events from Backend, needed for a
client synchronization, by its query method specialized for syncer server usage.

Syncer server is responsible for registering events in regard to connection
status of the specific syncer client. Events are defined with unique event type:

    * 'event', 'syncer', <client_name>

       where `<client_name>` is name of the
       syncer client defined as `clientName` in `MsgReq` message (see `Syncer
       communication`_).

        * `source_timestamp` is ``None``

        * `payload` is specified by
          ``hat-event://main.yaml#/definitions/events/syncer``.

This event is registered on behalf of syncer server with `Source.type` set to
``SYNCER`` (for the sake of simplicity hereafter we consider as syncer server
registers this event).

Payload of this event defines three states of a client connection:
``CONNECTED``, ``DISCONNECTED`` and ``SYNCED``. Once server receives
`MsgReq` from client, only then, server registers event with payload
``CONNECTED``. Once Server has sent all the events that client requested to
get "synchronized", it sends `MsgSynced` message and also registers event with
payload `SYNCED`. Once connection to a Client is lost, event with payload
``DISCONNECTED`` is registered by the Server (see `Syncer
communication`_ for more details about messages exchanged with Client).

Multiple clients can be connected to a server, where each connection is handled
independently, as described.


Engine
''''''

Engine is responsible for creating `Modules`_ and coordinating event
registration, executing processing sessions and `Backend`_.

Engine's method `register` enables registration of events to Backend based on a
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
eventer server, syncer server or a module requests new registration through
`register` method. After processing session ends, events are registered with
Backend. Start and end of each session is notified to each module by
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
          ``hat-event://main.yaml#/definitions/events/engine``.


Modules
'''''''

.. warning::

    Event server does not provide sandbox environment for loading end executing
    modules. Modules have full access to Event Server functionality which is
    controlled with module execution. Module implementation and configuration
    should be written in accordance to other modules and Event Server as a
    whole, keeping in mind processing execution time overhead and possible
    interference between modules.

Each module represents predefined and configurable closely related functions
that can modify the process of registering new events or initiate new event
registration sessions. When created, module is provided with reference to
`Engine`_ which can be used for registering and querying events and it's own
unique source identification (used in case of registering new events).

Implementation of specific module can be maintained independently of Event
Server implementation. Location of Python module implementing Event Server's
module is defined by Event Server's configuration.


Backend
'''''''

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

    * `Python hat.event module <py_api/hat/event.html>`_


Eventer chatter messages
------------------------

.. literalinclude:: ../schemas_sbs/eventer.sbs
    :language: none


Syncer chatter messages
-----------------------

.. literalinclude:: ../schemas_sbs/syncer.sbs
    :language: none


Configuration
-------------

.. literalinclude:: ../schemas_json/main.yaml
    :language: yaml


.. |arr| unicode:: U+003E

.. external references

.. _Monitor Server: https://hat-monitor.hat-open.com/monitor.html
.. _Component Information: https://hat-monitor.hat-open.com/monitor.html#component-information
.. _Chatter: https://hat-chatter.hat-open.com/chatter.html
