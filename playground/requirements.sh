#!/bin/sh

. $(dirname -- "$0")/env.sh

hat-json-convert $ROOT_PATH/pyproject.toml | \
jq -r '.project | .dependencies[], .["optional-dependencies"].dev[]'
