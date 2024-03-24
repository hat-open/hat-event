.. _backends:

Backends
========

Backend is wrapper for storing and retrieving events from specialized storage
engines.

Backends available as part of `hat-event` package:

    .. toctree::
       :maxdepth: 1

       dummy
       lmdb

Other backend implementations can be maintained independently of Event
Server implementation. Location of Python module implementing backend is
defined by Event Server's configuration.
