#!/bin/sh

set -e

cd $(dirname -- "$0")

export PYTHONPATH=../../src_py

exec python main.py "$@"
