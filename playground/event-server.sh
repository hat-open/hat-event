#!/bin/sh

set -e

PLAYGROUND_PATH=$(dirname "$(realpath "$0")")
. $PLAYGROUND_PATH/env.sh

LOG_LEVEL=DEBUG
CONF_PATH=$DATA_PATH/event.yaml

cat > $CONF_PATH << EOF
type: event
log:
    version: 1
    formatters:
        console_formatter:
            format: "[%(asctime)s %(levelname)s %(name)s] %(message)s"
        syslog_formatter: {}
    handlers:
        console_handler:
            class: logging.StreamHandler
            formatter: console_formatter
            level: DEBUG
        syslog_handler:
            class: hat.syslog.handler.SyslogHandler
            host: '127.0.0.1'
            port: 6514
            comm_type: TCP
            level: DEBUG
            formatter: syslog_formatter
    loggers:
        hat.event:
            level: $LOG_LEVEL
    root:
        level: INFO
        handlers:
            - console_handler
            - syslog_handler
    disable_existing_loggers: false
monitor:
    name: event
    group: event
    monitor_address: "tcp+sbs://127.0.0.1:23010"
backend:
    module: hat.event.server.backends.dummy
engine:
    server_id: 1
    modules: []
eventer_server:
    address: "tcp+sbs://127.0.0.1:23012"
syncer_server:
    address: "tcp+sbs://127.0.0.1:23013"
mariner_server:
    address: "tcp://127.0.0.1:23014"
EOF

exec $PYTHON -m hat.event.server \
    --conf $CONF_PATH \
    "$@"
