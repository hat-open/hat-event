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

        A client can, at any time, subscribe to events of certain types by
        sending a subscribe message (`MsgSubscribe`) to the server. After
        server receives subscribe message, it will spontaneously notify the
        client (by sending `MsgNotify`) whenever an event occurs with the
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
        end Chatter conversation (`MsgRegisterReq` last flag is ``false``),
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

        * `server_id` - optional filter condition

            If set, only events with `event_id.server` equal to `server_id`
            are matched.

        * `event_ids` - optional filter condition

            If set, only events with ids which are defined as part of filter
            condition are matched.

        * `event_types` - optional filter condition

            List of event types. If set, event type has to match at least one
            type from the list. Matching is done as defined in event's **type**
            property description - including the ``?`` and ``*`` options.

        * `t_from` - from timestamp, optional filter condition

            If set, only events with `timestamp` greater than or equal are
            matched.

        * `t_to` - to timestamp, optional filter condition

            If set, only events with `timestamp` lower than or equal are
            matched.

        * `source_t_from` - from source timestamp, optional filter condition

            If set, only events with `source timestamp` defined, and greater
            than or equal, are matched.

        * `source_t_to` - to source timestamp, optional filter condition

            If set, only events with `source timestamp` defined, and lower
            than or equal, are matched.

        * `payload` - optional filter condition

            If set, only events with `payload` defined and whose `payload`
            is the same as the query's `payload` are matched.

        * `order`

            Can be set to ``ascending`` or ``descending``. If set to
            ``ascending``, matched events will be returned ordered from the
            earliest to the latest dependent on their `timestamp` or
            `source timestamp` (this choice is determined by the `order by`
            property of the query). Earliest meaning lower timestamp, latest
            meaning greater timestamp. If set to descending the same logic
            applies, but the order is reversed.

        * `order_by`

            Can be set to ``timestamp`` or ``source timestamp``. Ordering events
            by ``source timestamp`` has events with `source timestamp` undefined
            returned last in an arbitrary order.

        * `unique_type`

            If set to ``true``, it determines whether the matched events will
            contain only one event instance of the same type. With the query
            `order` set to ``descending``, only one event with the greatest
            `timestamp` or `source timestamp` will be matched. Setting the
            `order` to ``ascending`` will match the event with the lowest
            `timestamp` or `source timestamp`.

        * `max_results`

            If set, limits the number of matched events to this number. Matched
            events are dependent on the query `order` the same way as in
            `unique_type`.


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
