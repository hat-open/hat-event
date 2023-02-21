.. _syncer:

Syncer
======

Syncer communication protocol enables communication between Syncer Server and
Syncer Client, based on `Chatter`_ communication. It is aimed primarily for
events synchronization between two, or more redundant
:ref:`Event Servers <event-server>`. Thus, each Event Server implements both
server and client side of this communication. Generally, this communication is
made not to be exclusively used within an Event Server: Syncer Client can be
used to establish synchronization of any remote instance (e.g. an arbitrary
database) with an Event Server.

Messages used in Syncer communication, defined in `HatSyncer` SBS
module (see `Chatter messages`_), are the following:

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
    | MsgFlushReq        | T     | F    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgFlushRes        | F     | T    | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+

Communication between Syncer Client and server starts with synchronization
request initiated by the client with `MsgReq` message. The connection between
server and client is considered established when `MsgReq` message is received
on the server side. With this message, client requests for server events in
order to get "synchronized". More specifically, by `MsgReq` client represents
itself by `clientName` and requests all the events that have `EventId` such
that `session` is same or greater and `instance` is greater (ie. newer) than
`lastEventId` and `server` corresponds to Syncer Server's Event Server.

Upon receiving `MsgReq`, server communicates back the requested events by
`MsgEvents` message. Events are grouped in `MsgEvents` such that all events in
the message are from the same session (`session` of `EventId`). Once all of
these events are sent, server sends `MsgSynced` message in order to signalize
the client that synchronization phase ended.

All events created (by the Event Server with running Syncer Server) after the
`MsgReq` (and before `MsgSynced`) are not included in the synchronization phase.
They are buffered on the server side and sent to client only after the
`MsgSynced` message, using the same `MsgEvents` message. In the remaining life
of the connection between Syncer Server and client, only `MsgEvents` messages
are sent from server to a client in order to notify client with the new events.
All events in one `MsgEvents` message always belong to the same session.

.. todo::

    add `subscription` to `MsgReq`.


Implementation
--------------

Documentation is available as part of generated API reference:

    * `Python hat.event module <py_api/hat/event/syncer.html>`_


Chatter messages
----------------

.. literalinclude:: ../schemas_sbs/syncer.sbs
    :language: none


.. |arr| unicode:: U+003E
.. _Chatter: https://hat-chatter.hat-open.com/chatter.html
