"""Event server main"""

from pathlib import Path
import argparse
import asyncio
import contextlib
import sys

from hat import aio
from hat.drivers import tcp

from hat.event import common


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--host', type=str, default='127.0.0.1',
        help="server host name (default '127.0.0.1')")
    parser.add_argument(
        '--port', type=int, default='23012',
        help="server TCP port (default 23012)")
    parser.add_argument(
        '--client-name', metavar='NAME', type=str, default='manager',
        help="client name (default 'manager')")
    parser.add_argument(
        '--client-token', metavar='TOKEN', type=str, default=None,
        help="client token")
    subparsers = parser.add_subparsers(
        title='actions', dest='action', required=True)

    register_parser = subparsers.add_parser(
        'register', description="register new event")
    register_parser.add_argument(
        '--source-timestamp', metavar='TIMESTAMP', type=_parse_timestamp,
        default=None,
        help="source timestamp")
    register_parser.add_argument(
        '--payload-type', metavar='TYPE',
        choices=['json', 'yaml', 'binary', 'none'],
        default=None,
        help="payload type")
    register_parser.add_argument(
        '--payload-path', metavar='PATH', type=Path, default=Path('-'),
        help="payload file path or '-' for stdin (default '-')")
    register_parser.add_argument(
        'event-type',
        help="event type where segments are delimited by '/'")

    query_parser = subparsers.add_parser(
        'query', description="query events")
    query_subparsers = query_parser.add_subparsers(
        title='query types', dest='query_type', required=True)

    query_latest_parser = query_subparsers.add_parser(
        'latest', description="query latest events")
    query_latest_parser.add_argument(
        '--event-types', metavar='EVENT_TYPE', type=_parse_event_type,
        default=None, nargs='*',
        help='query event types')

    query_timeseries_parser = query_subparsers.add_parser(
        'timeseries', description="query timeseries events")
    query_timeseries_parser.add_argument(
        '--event-types', metavar='EVENT_TYPE', type=_parse_event_type,
        default=None, nargs='*',
        help='query event types')
    query_timeseries_parser.add_argument(
        '--t-from', metavar='TIMESTAMP', type=_parse_timestamp, default=None,
        help="from timestamp")
    query_timeseries_parser.add_argument(
        '--t-to', metavar='TIMESTAMP', type=_parse_timestamp, default=None,
        help="to timestamp")
    query_timeseries_parser.add_argument(
        '--source-t-from', metavar='TIMESTAMP', type=_parse_timestamp,
        default=None,
        help="from source timestamp")
    query_timeseries_parser.add_argument(
        '--source-t-to', metavar='TIMESTAMP', type=_parse_timestamp,
        default=None,
        help="to source timestamp")
    query_timeseries_parser.add_argument(
        '--order', type=common.Order, choices=[i.name for i in common.Order],
        default=common.Order.DESCENDING,
        help="order (default 'DESCENDING')")
    query_timeseries_parser.add_argument(
        '--order-by', type=common.OrderBy,
        choices=[i.name for i in common.OrderBy],
        default=common.OrderBy.TIMESTAMP,
        help="order (default 'TIMESTAMP')")
    query_timeseries_parser.add_argument(
        '--max-results', metavar='N', type=int, default=None,
        help="maximum number of results")
    query_timeseries_parser.add_argument(
        '--last-event-id', metavar='SERVER_ID,SESSION_ID,INSTANCE_ID',
        type=_parse_event_id, default=None,
        help="last event id")

    query_server_parser = query_subparsers.add_parser(
        'server', description="query server events")
    query_server_parser.add_argument(
        '--persisted', action='store_true',
        help="persisted events")
    query_server_parser.add_argument(
        '--max-results', metavar='N', type=int, default=None,
        help="maximum number of results")
    query_server_parser.add_argument(
        '--last-event-id', metavar='SERVER_ID,SESSION_ID,INSTANCE_ID',
        type=_parse_event_id, default=None,
        help="last event id")
    query_server_parser.add_argument(
        'server_id', metavar='SERVER_ID', type=int,
        help="server id")

    subscribe_parser = subparsers.add_parser(
        'subscribe', description="watch newly registered events")
    subscribe_parser.add_argument(
        '--persisted', action='store_true',
        help="persisted events")
    subscribe_parser.add_argument(
        'event_types', metavar='EVENT_TYPE', type=_parse_event_type, nargs='+',
        help='query event type')

    server_parser = subparsers.add_parser(
        'server', description="run manager server with web ui")
    server_parser.add_argument(
        '--persisted', action='store_true',
        help="persisted events")
    server_parser.add_argument(
        'event_types', metavar='EVENT_TYPE', type=_parse_event_type, nargs='*',
        help='query event type')

    return parser


def main():
    parser = create_argument_parser()
    args = parser.parse_args()

    addr = tcp.Address(args.host, args.port)

    if args.action == 'register':
        payload = _read_payload(args.payload_type, args.payload_path)
        co = register(addr=addr,
                      client_name=args.client_name,
                      client_token=args.client_token,
                      event_type=args.event_type,
                      source_timestamp=args.source_timestamp,
                      payload=payload)

    elif args.action == 'query':
        if args.query_type == 'latest':
            raise NotImplementedError()

        elif args.query_type == 'timeseries':
            raise NotImplementedError()

        elif args.query_type == 'server':
            raise NotImplementedError()

        else:
            raise ValueError('unsupported query type')

    elif args.action == 'subscribe':
        raise NotImplementedError()

    elif args.action == 'server':
        raise NotImplementedError()

    else:
        raise ValueError('unsupported action')

    aio.init_asyncio()
    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(co)


async def register(addr: tcp.Address,
                   client_name: str,
                   client_token: str | None,
                   event_type: common.EventType,
                   source_timestamp: common.Timestamp | None,
                   payload: common.EventPayload | None):
    raise NotImplementedError()


def _parse_timestamp(t):
    return common.timestamp_from_float(float(t))


def _parse_event_type(event_type):
    return tuple(event_type.split('/'))


def _parse_event_id(event_id):
    return common.EventId(event_id.split(','))


def _read_payload(payload_type, path):
    raise NotImplementedError()


if __name__ == '__main__':
    sys.argv[0] = 'hat-event-manager'
    sys.exit(main())
