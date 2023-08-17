.. _modules:

Modules
=======

.. warning::

    Event Server does not provide sandbox environment for loading end executing
    modules. Modules have full access to Event Server functionality which is
    controlled with module execution. Module implementation and configuration
    should be written in accordance to other modules and Event Server as a
    whole, keeping in mind processing execution time overhead and possible
    interference between modules.

Each module represents predefined and configurable closely related functions
that can modify the process of registering new events or initiate new event
registration sessions. When created, module is provided with reference to
:ref:`Engine <server-engine>` which can be used for registering and querying
events and its own unique source identification (used in case of registering
new events).

Implementation of specific module can be maintained independently of Event
Server implementation. Location of Python module implementing Event Server's
module is defined by Event Server's configuration.
