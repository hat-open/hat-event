#!/bin/sh

set -e

PLAYGROUND_PATH=$(dirname "$(realpath "$0")")
. $PLAYGROUND_PATH/env.sh

LOG_LEVEL=DEBUG
CONF_PATH=$DATA_PATH/monitor.yaml

cat > $CONF_PATH << EOF
type: monitor
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
            host: "127.0.0.1"
            port: 6514
            comm_type: TCP
            level: DEBUG
            formatter: syslog_formatter
    loggers:
        hat.monitor:
            level: $LOG_LEVEL
    root:
        level: INFO
        handlers:
            - console_handler
            - syslog_handler
    disable_existing_loggers: false
default_algorithm: BLESS_ONE
group_algorithms: {}
server:
    host: "127.0.0.1"
    port: 23010
    default_rank: 1
master:
    host: "127.0.0.1"
    port: 23011
slave:
    parents: []
    connect_timeout: 5
    connect_retry_count: 3
    connect_retry_delay: 5
ui:
    host: "127.0.0.1"
    port: 23022
EOF

exec $PYTHON -m hat.monitor.server \
    --conf $CONF_PATH \
    "$@"
