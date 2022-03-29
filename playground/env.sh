#!/bin/sh

PYTHON=${PYTHON:-python}
RUN_PATH=$(cd $(dirname -- "$0") && pwd)
ROOT_PATH=$RUN_PATH/..
DATA_PATH=$RUN_PATH/data
DIST_PATH=$RUN_PATH/dist

export PYTHONPATH=$ROOT_PATH/src_py

mkdir -p $DATA_PATH
