#!/bin/sh

. $(dirname -- "$0")/env.sh


exec $PYTHON -m hat.event.server.backends.lmdb.manager "$@"
