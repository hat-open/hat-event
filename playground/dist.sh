#!/bin/sh

set -e

. $(dirname -- "$0")/env.sh

TARGETS="linux_gnu_x86_64:cp38
         linux_gnu_x86_64:cp39
         linux_gnu_x86_64:cp310
         linux_gnu_x86_64:cp311
         linux_musl_x86_64:cp38
         linux_musl_x86_64:cp39
         linux_musl_x86_64:cp310
         linux_musl_x86_64:cp311
         linux_gnu_aarch64:cp39
         linux_gnu_aarch64:cp38
         linux_gnu_aarch64:cp310
         linux_gnu_aarch64:cp311
         windows_amd64:cp38
         windows_amd64:cp39
         windows_amd64:cp310
         windows_amd64:cp311"
# TARGETS=""

cd $ROOT_PATH
rm -rf $DIST_PATH
mkdir -p $DIST_PATH

for TARGET in $TARGETS; do
    export TARGET_PLATFORM=$(echo $TARGET | cut -d ':' -f 1)
    export TARGET_PY_VERSION=$(echo $TARGET | cut -d ':' -f 2)
    $PYTHON -m doit clean_all
    $PYTHON -m doit
    cp $ROOT_PATH/build/py/dist/*.whl $DIST_PATH
done

IMAGES="linux/arm/v7/build-hat-event:debian11-cpy3.8
        linux/arm/v7/build-hat-event:debian11-cpy3.9
        linux/arm/v7/build-hat-event:debian11-cpy3.10
        linux/arm/v7/build-hat-event:debian11-cpy3.11"
IMAGES="linux/arm/v7/build-hat-event:debian11-cpy3.10"
# IMAGES="linux/arm/v7/build-hat-event:debian11-cpy3.11"

for IMAGE in $IMAGES; do
    $PYTHON -m doit clean_all
    PLATFORM=$(dirname $IMAGE)
    IMAGE_ID=$(podman images -q $IMAGE)
    podman build --platform $PLATFORM \
                 -f $RUN_PATH/dockerfiles/$IMAGE \
                 -t $IMAGE \
                 .
    if [ -n "$IMAGE_ID" -a "$IMAGE_ID" != "$(podman images -q $IMAGE)" ]; then
        podman rmi $IMAGE_ID
    fi
    podman run --rm \
               --platform $PLATFORM \
               -v $DIST_PATH:/hat/dist \
               -v ~/.cache/pip:/root/.cache/pip \
               -i $IMAGE /bin/sh - << EOF
set -e
python3 -m venv venv
. venv/bin/activate
export CARGO_NET_GIT_FETCH_WITH_CLI=true  # cryptography
pip install --upgrade pip
pip install --upgrade -r requirements.pip.dev.txt
doit clean_all
doit
cp build/py/dist/*.whl dist
EOF
done

$PYTHON -m doit clean_all
