#!/bin/sh

set -e

. $(dirname -- "$0")/env.sh

for i in $(find $ROOT_PATH/src_py -name '*.so'); do
    objdump -T $i | grep GLIBC
done
