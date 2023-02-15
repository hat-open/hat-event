.. _mariner:

Mariner
=======

Mariner communication protocol enables communication between Mariner Server
and Mariner Client, based on TCP communication with JSON encoded messages.
It is desined as communication interface between Hat-based systems and
external applications. Exchange of data is based on sending/receiving
:ref:`event <event>` structures.


Transport
---------

Mariner Server and Mariner Client (in the remainder of this document, mostly
referred only as `server` and `client`) communicate by establishing
TCP connection. Server opens listening socket and accepts new TCP connections
initiated by client. In addition to non encrypted communication, TCP stream
can be wrapped in SSL layer. Both server and client can close TCP connection
at any time, thus closing Mariner communication.

For each connection, underlying TCP stream is segmented into blocks. Each block
contains a single communication message prefixed by its message header. This
header consists of `1+m` bytes where the first byte contains the value `m`
(header byte length), while following `m` bytes contain value `k` (message byte
length) encoded as big-endian (the first byte value does not include the first
header byte and message length does not include header length). Message itself
is utf-8 encoded JSON value.

Visualization of the communication stream::

    address    ...  |  n  | n+1 |  ...  | n+m | n+m+1 |  ...  | n+m+k |  ...
             -------+-----+-----+-------+-----+-------+-------+-------+-------
       data    ...  |  m  |         k         |        message        |  ...
             -------+-----+-----+-------+-----+-------+-------+-------+-------

where:

    * `m` is byte length of `k` (header length without first byte)
    * `k` is byte length of `message` (`n+1` is the most significant byte)
    * `message` is JSON encoded message


Messages
--------

Communication between server and client is based on full-duplex message
passing where both server and client can send/receive messages at any time.
Single message is represented with JSON Object with mandatory property ``type``
(value of this property is always string). Property with key ``type``
represents type of communication message. Existence and semantics of other
properties is dependent of message type. Structure of messages is defined
by JSON Schema ``hat-event://mariner.yaml#`` (see
`JSON Schema definitions`_).

Supported messages (identified by message type) are:

    * `ping`

        Can be sent by both server and client. Once server or client received
        this message, it should immediately send `pong` message.

    * `pong`

        Can be sent by both server and client. Represents response to `ping`
        message`.

    * `init`

        Initial message sent by client. This message can be sent only once,
        immediately after establishment of TCP connection. This message
        identifies client and contains additional initialization parameters:

            * `client_id`

                Client identifier.

            * `client_token`

                Optional client identification token.

            * `last_event_id`

                Optional identification of last event available on client
                side. If this property is not provided, server will send only
                newly created events without notification of previously
                created and persisted events.

            * `subscriptions`

                List of subscribed event typed (see :ref:`event <event>`).

    * `events`

        Message sent by server. Single `events` message contains all events
        associated with single Event Server session.

.. todo::

    * multiple last_event_ids in init message (multiple server instances)
    * synced message


Communication
-------------

...


Implementation
--------------

Documentation is available as part of generated API reference:

    * `Python hat.event.mariner module <py_api/hat/event/mariner.html>`_


JSON Schema definitions
-----------------------

.. literalinclude:: ../schemas_json/mariner.yaml
    :language: yaml
