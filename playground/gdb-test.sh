#!/bin/sh

set -e

PLAYGROUND_PATH=$(dirname "$(realpath "$0")")
. $PLAYGROUND_PATH/env.sh

PYTHON_BIN="$($PYTHON -c "import sys; print(sys.executable)")"

cd $ROOT_PATH
doit clean_all
doit json_schema_repo sbs_repo csubscription

cd $ROOT_PATH/test_pytest
exec gdb --directory $ROOT_PATH \
         --command $PLAYGROUND_PATH/gdb-test.gdb \
         --args $PYTHON_BIN -m pytest -s -p no:cacheprovider "$@"
