#!/bin/sh

set -e

RUN_PATH=$(dirname "$(realpath "$0")")
PLAYGROUND_PATH=$RUN_PATH/../..
. $PLAYGROUND_PATH/env.sh


db_path=$DATA_PATH/lmdb_timeseries.db

exec $PYTHON $RUN_PATH/query.py $db_path
