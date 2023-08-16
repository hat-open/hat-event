#!/bin/sh

. $(dirname -- "$0")/env.sh

$PYTHON - << EOF
from pathlib import Path
from hat import json

conf = json.decode_file(Path("$ROOT_PATH/pyproject.toml"))
for i in [*conf['project']['dependencies'],
          *conf['project']['optional-dependencies']['dev']]:
    print(i)
EOF
