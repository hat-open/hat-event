.. _event:

Event
=====

Event is a generic data structure used for communication and storage of
relevant changes happening through time in a Hat system. For example, event can
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
  main identifier of the occurred event's type. This property is set by the
  user while registering an event. Subtypes are arbitrary strings formed from
  any characters excluding ``?``, ``*`` and ``/``.

* `timestamp`

  This property determines the moment when the event was registered on the
  server. It is always set by Event Server.

* `source timestamp`

  This property is optional. It represents the moment the Event occurred
  as detected by the source of the Event. It is always set by the
  Eventer Client.

* `payload`

  This property is optional. It can be used to provide additional data
  bound to an event. The payload property is always set by the client
  registering the event. Its contents are determined by the Event's
  `type` and can be decoded by clients who understand Events of that
  `type`. Payload can be encoded as binary or JSON data. Event Server doesn't
  decode binary payload while receiving event requests, storing events or
  providing query results. Such binary payloads can be optionally decoded by
  Event Server's modules.


.. _event-type:

Event type
----------

As previosly specified, event type is ordered sequence of subtypes where
each subtype is string of arbitrary characters (characters ``?``, ``*`` and
``/`` can't be part of subtype).

When used in querying and subscription, this property has additional
semantics. Any string in the sequence can be replaced with ``?`` while the
last string can also be replaced with ``*``. Replacements must
substitute an entire string in the list. The semantics of these
replacements are:

* The string ``?``

  is matched with a single arbitrary string.

* The string ``*``

  is matched with any number (zero or more) of arbitrary strings.

When event type should be encoded as single string, subtypes can be
concatenated together with character ``/`` as separator between subtypes.


.. _event-ordering:

Event ordering
--------------

Taking into account immutability and uniquely identification of events
(if two events have same `id`, they represent same event), natural ordering
between events ``e1`` and ``e2`` is defined as:

* if ``e1.id.server`` = ``e2.id.server``
  and ``e1.id.session`` = ``e2.id.session``
  and ``e1.id.instance`` < ``e2.id.instance``
  then ``e1`` < ``e2``

* else if ``e1.id.server`` = ``e2.id.server``
  and ``e1.id.session`` = ``e2.id.session``
  and ``e1.id.instance`` > ``e2.id.instance``
  then ``e1`` > ``e2``

* else if ``e1.id.server`` = ``e2.id.server``
  and ``e1.id.session`` < ``e2.id.session``
  then ``e1`` < ``e2``

* else if ``e1.id.server`` = ``e2.id.server``
  and ``e1.id.session`` > ``e2.id.session``
  then ``e1`` > ``e2``

* else if ``e1.timestamp`` < ``e2.timestamp``
  then ``e1`` < ``e2``

* else if ``e1.timestamp`` > ``e2.timestamp``
  then ``e1`` > ``e2``

* else if ``e1`` registered before ``e2``
  then ``e1`` < ``e2``

* else ``e1`` > ``e2``

This definition includes ambiguity regarding sequence of event registration
providing possibility that different Event Servers calculate different
order of same events.
