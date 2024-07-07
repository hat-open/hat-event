from collections.abc import Collection
from pathlib import Path
import argparse
import asyncio
import contextlib
import sys

from hat import aio
from hat import json
from hat.drivers import tcp

from hat.event import eventer
from hat.event.manager import common


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
        choices=['json', 'yaml', 'toml', 'binary', 'none'],
        default=None,
        help="payload type")
    register_parser.add_argument(
        '--binary-type', metavar='TYPE', type=str, default='',
        help="binary payload type (default '')")
    register_parser.add_argument(
        '--payload-path', metavar='PATH', type=Path, default=Path('-'),
        help="payload file path or '-' for stdin (default '-')")
    register_parser.add_argument(
        'event_type', metavar='EVENT_TYPE', type=_parse_event_type,
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
        '--order', type=_parse_order, choices=[i.name for i in common.Order],
        default=common.Order.DESCENDING,
        help="order (default 'DESCENDING')")
    query_timeseries_parser.add_argument(
        '--order-by', type=_parse_order_by,
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
        '--server-id', type=int, default=None,
        help="server id")
    subscribe_parser.add_argument(
        '--persisted', action='store_true',
        help="persisted events")
    subscribe_parser.add_argument(
        'event_types', metavar='EVENT_TYPE', type=_parse_event_type, nargs='*',
        help='query event type')

    server_parser = subparsers.add_parser(
        'server', description="run manager server with web ui")
    server_parser.add_argument(
        '--server-id', type=int, default=None,
        help="server id")
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
        register_event = common.RegisterEvent(
            type=args.event_type,
            source_timestamp=args.source_timestamp,
            payload=_read_payload(payload_type=args.payload_type,
                                  binary_type=args.binary_type,
                                  path=args.payload_path))

        co = register(addr=addr,
                      client_name=args.client_name,
                      client_token=args.client_token,
                      register_event=register_event)

    elif args.action == 'query':
        if args.query_type == 'latest':
            params = common.QueryLatestParams(
                event_types=args.event_types)

        elif args.query_type == 'timeseries':
            params = common.QueryTimeseriesParams(
                event_types=args.event_types,
                t_from=args.t_from,
                t_to=args.t_to,
                source_t_from=args.source_t_from,
                source_t_to=args.source_t_to,
                order=args.order,
                order_by=args.order_by,
                max_results=args.max_results,
                last_event_id=args.last_event_id)

        elif args.query_type == 'server':
            params = common.QueryTimeseriesParams(
                server_id=args.server_id,
                persisted=args.persisted,
                max_results=args.max_results,
                last_event_id=args.last_event_id)

        else:
            raise ValueError('unsupported query type')

        co = query(addr=addr,
                   client_name=args.client_name,
                   client_token=args.client_token,
                   params=params)

    elif args.action == 'subscribe':
        subscriptions = args.event_types or [('*', )]

        co = subscribe(addr=addr,
                       client_name=args.client_name,
                       client_token=args.client_token,
                       subscriptions=subscriptions,
                       server_id=args.server_id,
                       persisted=args.persisted)

    elif args.action == 'server':
        subscriptions = args.event_types or [('*', )]

        co = server(addr=addr,
                    client_name=args.client_name,
                    client_token=args.client_token,
                    subscriptions=subscriptions,
                    server_id=args.server_id,
                    persisted=args.persisted)

    else:
        raise ValueError('unsupported action')

    aio.init_asyncio()
    with contextlib.suppress(asyncio.CancelledError):
        return aio.run_asyncio(co)


async def register(addr: tcp.Address,
                   client_name: str,
                   client_token: str | None,
                   register_event: common.RegisterEvent):
    client = await eventer.connect(addr=addr,
                                   client_name=client_name,
                                   client_token=client_token)

    try:
        result = await client.register([register_event])

        if result is None:
            return 1

    finally:
        await aio.uncancellable(client.async_close())


async def query(addr: tcp.Address,
                client_name: str,
                client_token: str | None,
                params: common.QueryParams):
    client = await eventer.connect(addr=addr,
                                   client_name=client_name,
                                   client_token=client_token)

    try:
        result = await client.query(params)

        result_json = common.query_result_to_json(result)
        print(json.encode(result_json))

    finally:
        await aio.uncancellable(client.async_close())


async def subscribe(addr: tcp.Address,
                    client_name: str,
                    client_token: str | None,
                    subscriptions: Collection[common.EventType],
                    server_id: int | None,
                    persisted: bool):

    def on_events(client, events):
        events_json = [common.event_to_json(event) for event in events]
        print(json.encode(events_json))

    client = await eventer.connect(addr=addr,
                                   client_name=client_name,
                                   client_token=client_token,
                                   subscriptions=subscriptions,
                                   server_id=server_id,
                                   persisted=persisted,
                                   events_cb=on_events)

    try:
        await client.wait_closing()

    finally:
        await aio.uncancellable(client.async_close())


async def server(addr: tcp.Address,
                 client_name: str,
                 client_token: str | None,
                 subscriptions: Collection[common.EventType],
                 server_id: int | None,
                 persisted: bool):
    raise NotImplementedError()


def _parse_timestamp(t):
    if t == 'now':
        return common.now()

    return common.timestamp_from_float(float(t))


def _parse_event_type(event_type):
    return tuple(event_type.split('/'))


def _parse_event_id(event_id):
    return common.EventId(event_id.split(','))


def _parse_order(order):
    return common.Order[order]


def _parse_order_by(order_by):
    return common.OrderBy[order_by]


def _read_payload(payload_type, binary_type, path):
    if payload_type == 'none':
        return

    if path == Path('-'):
        if payload_type == 'json' or payload_type is None:
            json_format = json.Format.JSON

        elif payload_type == 'yaml':
            json_format = json.Format.YAML

        elif payload_type == 'toml':
            json_format = json.Format.TOML

        elif payload_type == 'binary':
            json_format = None

        else:
            raise ValueError('unsupported payload type')

        if json_format is None or json_format == json.Format.TOML:
            stdin, sys.stdin = sys.stdin.detach(), None

        else:
            stdin = sys.stdin

        if json_format:
            data = json.decode_stream(stdin, json_format)
            return common.EventPayloadJson(data)

        else:
            data = stdin.read()
            return common.EventPayloadBinary(type=binary_type,
                                             data=data)

    if payload_type is None:
        try:
            json_format = json.get_file_format(path)

        except ValueError:
            json_format = None

    elif payload_type == 'json':
        json_format = json.Format.JSON

    elif payload_type == 'yaml':
        json_format = json.Format.YAML

    elif payload_type == 'toml':
        json_format = json.Format.TOML

    elif payload_type == 'binary':
        json_format = None

    else:
        raise ValueError('unsupported payload type')

    if json_format:
        data = json.decode_file(path, json_format)
        return common.EventPayloadJson(data)

    data = path.read_bytes()
    return common.EventPayloadBinary(type=binary_type,
                                     data=data)


if __name__ == '__main__':
    sys.argv[0] = 'hat-event-manager'
    sys.exit(main())
