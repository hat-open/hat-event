: ${PLAYGROUND_PATH:?}

PYTHON=${PYTHON:-python3}
ROOT_PATH=$PLAYGROUND_PATH/..
DATA_PATH=$PLAYGROUND_PATH/data
DIST_PATH=$PLAYGROUND_PATH/dist

export PYTHONPATH=$ROOT_PATH/src_py

mkdir -p $DATA_PATH
