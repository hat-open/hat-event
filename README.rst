.. _online documentation: https://hat-event.hat-open.com
.. _git repository: https://github.com/hat-open/hat-event.git
.. _PyPI project: https://pypi.org/project/hat-event
.. _pydoit: https://pydoit.org
.. _Hat Open: https://hat-open.com
.. _Končar Digital: https://www.koncar.hr/en


hat-event - Event server and communication libraries
====================================================

For more information see:

* `online documentation`_
* `git repository`_


Runtime requirements
--------------------

* python >=3.10


Install
-------

`hat-event` is available as `PyPI project`_::

    $ pip install hat-event


Build
-----

To install editable installation, together with python development
dependencies, run::

    $ pip install -e '.[dev]'

To install only python development dependencies, run::

    $ pip install -r requirements.pip.txt

Build tool used for `hat-event` is `pydoit`_. For listing available doit tasks,
use::

    $ doit list

Default task::

    $ doit

creates wheel package inside `build` directory.


Hat Open
--------

`hat-event` is part of `Hat Open`_ project - open-source framework of
tools and libraries for developing applications used for remote monitoring,
control and management of intelligent electronic devices such as IoT devices,
PLCs, industrial automation or home automation systems.

Development of Hat Open and associated repositories is sponsored by
`Končar Digital`_.


License
-------

Copyright 2020-2024 Hat Open AUTHORS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
