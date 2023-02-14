.. _event:

Event
=====

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
                integer that starts from ``1``. Instance of value ``0`` is never
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
        property is set by the user while registering an event. Subtypes are
        arbitrary strings formed from valid utf-8 characters excluding
        characters ``?``, ``*`` and ``/``.

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
        server. It is always set by the server (that is by Engine).

    * `source timestamp`

        This property is optional. It represents the moment the Event occurred
        as detected by the source of the Event. It is always set by the
        Eventer Client.

    * payload

        This property is optional. It can be used to provide additional data
        bound to an event. The payload property is always set by the client
        registering the event. Its contents are determined by the Event's
        `type` and can be decoded by clients who understand Events of that
        `type`. Payload can be encoded as binary data, JSON data or SBS
        encoded data. Event Server doesn't decode payload while receiving
        event requests, storing events or providing query results. Payload
        can be optionally decoded by Event Server's modules.
