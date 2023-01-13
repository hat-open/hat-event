#!/bin/sh

set -e

. $(dirname -- "$0")/env.sh

TARGET_PLATFORMS="linux_gnu_x86_64
                  linux_gnu_aarch64
                  linux_musl_x86_64
                  windows_amd64"
TARGET_PY_VERSIONS="cp38
                    cp39
                    cp310"

cd $ROOT_PATH
rm -rf $DIST_PATH
mkdir -p $DIST_PATH

for TARGET_PLATFORM in $TARGET_PLATFORMS; do
    for TARGET_PY_VERSION in $TARGET_PY_VERSIONS; do
        export TARGET_PLATFORM TARGET_PY_VERSION
        $PYTHON -m doit clean_all
        $PYTHON -m doit
        cp $ROOT_PATH/build/py/dist/*.whl $DIST_PATH
    done
done

$PYTHON -m doit clean_all
