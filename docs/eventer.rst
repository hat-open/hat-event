.. _eventer:

Eventer
=======

Eventer communication protocol enables communication between Eventer Server and
Eventer Client (in the remainder of this document, mostly referred only as
`server` and `client`). It is based on `Chatter`_ communication, defined in
`HatEventer` SBS module (see `Chatter messages`_) with the following messages:

    +--------------------+----------------------+-----------+
    |                    | Conversation         |           |
    | Message            +-------+------+-------+ Direction |
    |                    | First | Last | Token |           |
    +====================+=======+======+=======+===========+
    | MsgInitReq         | T     | F    | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+
    | MsgInitRes         | F     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgStatusNotify    | T     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgEventsNotify    | T     | T/F  | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgEventsAck       | F     | T    | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+
    | MsgRegisterReq     | T     | T/F  | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+
    | MsgRegisterRes     | F     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgQueryReq        | T     | F    | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+
    | MsgQueryRes        | F     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+


Initialization
--------------

Immediately after chatter connection is established, client must send
`MsgInitReq` message. This message contains:

* client name

  Label used as client identification. By convention, client name is usually
  formatted as ``<type>/<name>`` where `<type>` represents component
  type and `<name>` represents component instance name. Both component type
  and instance name can contain ``/`` as additional segment delimiter.

* client token

  Optional client token. Additional client identification which can be used
  for authentication or state synchronization between client and server.

* subscriptions

  List of event types (including query subtypes) which is used as filter
  for future event notifications. Only events with types satisfying
  subscriptions will be notified to client.

* server id

  Optional identification of server used for filtering of notified events.
  Only events originating from specified server will be notified to client.
  If server id is not specified, all events (that satisfy subscription) are
  notified to client.

* persisted

  Flag indicating when should server send event notifications to client.
  If this property is ``true``, server should send notifications after
  events are persisted. If this property is ``false``, server should send
  notifications immediately after event registration.

When server receives `MsgInitReq`, server responds with `MsgInitRes` message.
If server declines establishment of new connection, it will respond with
``error`` response containing appropriate error description. In case of
successful connection establishment, server responds with ``success``
message containing current server's status. Eventer connection is considered
established only after `MsgInitRes` ``success`` is sent. In case of
`MsgInitRes` ``error``, server should wait for client to receive message and
initiate closing of chatter connection (this waiting should be constrained
with timeout period after which server closes chatter connection regardless
of client behavior).

Initiation token recommendations for Event Server implementation:

* When server receives `MsgInitReq` without client token, it should
  accept connection.

* When server receives `MsgInitReq` with client token, it should compare
  client token to server token specified by server configuration. Only if this
  tokens match, connection should be accepted.


Server status
-------------

Event Server execution status can be classified as ``standby`` and
``operational``. Difference between these two statuses doesn't impact
Eventer communication. Only difference that client can expect is that
event registration during ``standby`` status will usually fail.

As part of successful Eventer connection establishment, server notifies it's
current status to client. During Eventer communication, server can at any time
send `MsgStatusNotify` message and thus notify client of it's status change.


Events notification
-------------------

After Eventer connection is established, server can at any time send
`MsgEventsNotify` message. Events in single `MsgEventsNotify` contain only
events from same session (single session's events should not be split into
multiple `MsgEventsNotify` messages). Before events are sent to specific
client, they are additionally filtered using properties negotiated during
client's connection initialization.

`MsgEventsNotify` can be sent with `last` flag set to ``true`` or ``false``.
If client receives `MsgEventsNotify` with `last` set to ``False``, it should
respond with ``MsgEventsAck`` message.


Event registration
------------------

After Eventer connection is established, client can at any time send
`MsgRegisterReq` message, requesting event registration. When server
receives registration request, it should create new events. If
`MsgRegisterReq` is not marked as last message in chatter conversation,
server should send `MsgRegisterRes` message reporting registration status.
If event registration is successful, `MsgRegisterRes` will contain
newly created events associated with ones from register request
(association is based on event's index in event list).


Event querying
--------------

After Eventer connection is established, client can at any time send
`MsgQueryReq` message. When server receives `MsgQueryReq`, it should respond
with `MsgQueryRes` message containing query results.

Supported query types:

* latest

  Query single latest events of each specified event type. Latest event
  is considered greatest event by event natural ordering (see
  :ref:`event-ordering`). This query includes:

  * event types

    List of queried event types include wild-char subtypes (see
    :ref:`event-type`). If this property is not set, all event types are
    queried (same as ``*``).

* timeseries

  Query sequence of events ordered by timestamp or source timestamp.
  If multiple events have same timestamp or source timestamp, event natural
  ordering is used as secondary order (see :ref:`event-ordering`). This query
  includes:

  * event types

    List of queried event types include wild-char subtypes (see
    :ref:`event-type`). If this property is not set, all event types are
    queried (same as ``*``).

  * from timestamp

    Inclusive lower limit for event timestamp (query events which have
    timestamp greater or equal to specified value). If this property is not
    set, lower limit is not set.

  * to timestamp

    Inclusive upper limit for event timestamp (query events which have
    timestamp less or equal to specified value). If this property is not
    set, upper limit is not set.

  * from source timestamp

    Inclusive lower limit for event source timestamp (query events which have
    source timestamp greater or equal to specified value). If this property is
    not set, lower limit is not set.

  * to source timestamp

    Inclusive upper limit for event source timestamp (query events which have
    source timestamp less or equal to specified value). If this property is not
    set, upper limit is not set.

  * order

    Property specifying choice between ascending or descending ordering.

  * order by

    Property specifying which property is used as primary ordering value -
    timestamp or source timestamp.

  * max results

    Maximum number events in query result. This value is optionally defined by
    client. Server should include it's own restriction on maximum number of
    resulting events (real maximum number of events could be smaller than one
    requested by client).

  * last event id

    Optional event identifier. If provided, this event is used as sentinel
    value for discarding events from query result. When server filters and
    orders events which should be included in query result, it should
    sequentially discard events until event id with provided value occurs.
    Once specified value is found, rest of events are reported as query result
    with application of `max results` constraint. Event with specified last
    event id is not part of query result. This property, together with
    `max results`, enables client driven query pagination.

* server

  Query events associated with specific `server id` in ascending natural
  order (see :ref:`event-ordering`). This query includes:

  * server id

    Identification of server which registered queried events.

  * persisted

    If this property is set to ``true``, only events that are successfully
    persisted are returned. Otherwise, all registered events are returned.

  * max results

    Maximum number events in query result. This value is optionally defined by
    client. Server should include it's own restriction on maximum number of
    resulting events (real maximum number of events could be smaller than one
    requested by client).

  * last event id

    Optional event identifier. If provided, only events greater by definition
    of natural ordering are returned (see :ref:`event-ordering`). Event with
    specified last event id is not part of query result. This property,
    together with `max results`, enables client driven query pagination.

All queries result with:

* events

  List of events satisfing query filters.

* more follows

  Flag indicating existence of more events that could be included as
  part of query result but were omitted due to `max results` constraint.


Implementation
--------------

Documentation is available as part of generated API reference:

    * `Python hat.event.eventer module <py_api/hat/event/eventer.html>`_


Chatter messages
----------------

.. literalinclude:: ../schemas_sbs/eventer.sbs
    :language: none


.. |arr| unicode:: U+003E
.. _Chatter: https://hat-chatter.hat-open.com/chatter.html
