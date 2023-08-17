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
    | MsgInitReq         | T     | F    | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+
    | MsgInitRes         | F     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgSynced          | T     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgEvents          | T     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgFlushReq        | T     | F    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+
    | MsgFlushRes        | F     | T    | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+

Communication between Syncer Client and server starts with initialization
request initiated by the client with `MsgInitReq` message. When Syncer Server
receives `MsgInitReq`, it must immediately respond with `MsgInitRes` message.
The connection between server and client is considered established when
client receives `MsgInitRes` success message. If `MsgInitRes` error is
received, chatter connection should be closed. With `MsgInitReq` message,
client requests for server events in order to get "synchronized". More
specifically, by `MsgInitReq` client represents itself by `clientName` and
requests all the events that have `EventId` such that `session` is same or
greater and `instance` is greater (ie. newer) than `lastEventId` and `server`
corresponds to Syncer Server's Event Server.

Upon receiving `MsgInitReq` and responding with `MsgInitRes`, server
communicates back the requested events by `MsgEvents` message. Events are
grouped in `MsgEvents` such that all events in the message are from the same
session (`session` of `EventId`). Once all of these events are sent, server
sends `MsgSynced` message in order to signalize the client that synchronization
phase ended.

All events created (by the Event Server with running Syncer Server) after the
`MsgInitReq` (and before `MsgSynced`) are not included in the synchronization
phase. They are buffered on the server side and sent to client only after the
`MsgSynced` message, using the same `MsgEvents` message. In the remaining life
of the connection between Syncer Server and client, only `MsgEvents` messages
are sent from server to a client in order to notify client with the new events.
All events in one `MsgEvents` message always belong to the same session.

In addition to `lastEventId`, `MsgInitReq` contains `subscriptions` and
optional `clientToken`. `subscription` is used as additional filter for
reporting only events that satisfy specified event types. `clientToken`, if
configured by server, is used for identifying clients which are allowed to
communicate to server. This parameter is usually used for checking
configuration synchronization between client and server.


Implementation
--------------

Documentation is available as part of generated API reference:

    * `Python hat.event.syncer module <py_api/hat/event/syncer.html>`_


Chatter messages
----------------

.. literalinclude:: ../schemas_sbs/syncer.sbs
    :language: none


.. |arr| unicode:: U+003E
.. _Chatter: https://hat-chatter.hat-open.com/chatter.html
