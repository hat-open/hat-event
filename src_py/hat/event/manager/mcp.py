import functools

from hat import aio
from hat import juggler
from hat.drivers import mcp
from hat.drivers import net

from hat.event import eventer
from hat.event.manager import common
from hat.event.manager import rest


async def run(addr: net.StreamAddress,
              client_name: str,
              client_token: str | None,
              http_addr: net.TcpAddress):
    async_group = aio.Group()

    try:
        client = await eventer.connect(addr=addr,
                                       client_name=client_name,
                                       client_token=client_token)
        await _bind_resource(async_group, client)

        server = await _create_web_server(http_addr=http_addr,
                                          client=client)
        await _bind_resource(async_group, server)

        await async_group.wait_closing()

    finally:
        await aio.uncancellable(async_group.async_close())


async def _create_web_server(http_addr, client):
    mcp_server = mcp.Server(
        name='hat-event',
        instructions="Hat Open Event Server interface for querying events. "
                     "Event is a generic data structure used for "
                     "communication and storage of relevant changes happening "
                     "through time in a Hat system. Each event is immutable "
                     "and uniquely identified by its event id. Event "
                     "contains: id (unique event identifier), type (list of "
                     "strings used as main identifier of event's type), "
                     "timestamp (moment when event was registered), source "
                     "timestamp (optional time provided by event's source), "
                     "payload (optional data with structure defined by "
                     "event's type).")

    mcp_server.add_tool(
        mcp.Tool(
            name='query_latest',
            title='Query latest events',
            description="Return the most recent event for each event type "
                        "matching the filters. Events are returned from "
                        "latest storage. Use this to read the current "
                        "state of something. Results are not paginated. "
                        "Omitting event_types returns the latest event of "
                        "every type",
            input_schema=_query_latest_params_schema,
            output_schema=_query_result_schema,
            call_cb=functools.partial(_process_query, client,
                                      rest.parse_latest_params)))

    mcp_server.add_tool(
        mcp.Tool(
            name='query_timeseries',
            title='Query event history',
            description="Return historical events from timeseries storage. "
                        "Events are filtered by event type and time range, "
                        "sorted by registration timestamp or source "
                        "timestamp. Paginate with last_event_id while "
                        "more_follows is true.",
            input_schema=_query_timeseries_params_schema,
            output_schema=_query_result_schema,
            call_cb=functools.partial(_process_query, client,
                                      rest.parse_timeseries_params)))

    mcp_server.add_tool(
        mcp.Tool(
            name='query_server',
            title='Query events by originating server',
            description="Return all events registered by a specific server in "
                        "registration order. Paginate with last_event_id "
                        "while more_follows is true",
            input_schema=_query_server_params_schema,
            output_schema=_query_result_schema,
            call_cb=functools.partial(_process_query, client,
                                      rest.parse_server_params)))

    return await juggler.listen(host=http_addr.host,
                                port=http_addr.port,
                                ws_path=None,
                                index_path=None,
                                additional_routes=[mcp_server.route])


async def _process_query(client, parse_params, args):
    try:
        params = parse_params(args)

    except Exception as e:
        raise Exception(f"parse params error: {e}")

    result = await client.query(params)

    return common.query_result_to_json(result)


async def _bind_resource(async_group, resource):
    try:
        async_group.spawn(aio.call_on_cancel, resource.async_close)
        async_group.spawn(aio.call_on_done, resource.wait_closing(),
                          async_group.close)

    except Exception:
        await aio.uncancellable(resource.async_close())
        raise


_query_event_type_schema = {
    'title': 'Query event type',
    'description': "Event type used for event querying. Can contain segments "
                   "'?' (as any segment) and '*' (only as last segment).",
    'type': 'array',
    'items': {
        'title': 'Query event type segment',
        'description': "Single query event type segment. '?' matches any "
                       "single event type segment. '*' matches zero or "
                       "more arbitrary event type segments.",
        'type': 'string'}}

_event_type_schema = {
    'title': 'Event type',
    'type': 'array',
    'items': {
        'title': 'Event type segment',
        'description': "Single event type segment. Values '*' and '?' are not "
                       "valid. Character '/' should not be used as part of "
                       "segment value.",
        'type': 'string'}}

_timestamp_schema = {
    'title': 'Timestamp',
    'description': 'Time elapsed from epoch (1970-01-01T00:00:00Z)',
    'type': 'object',
    'required': ['s', 'us'],
    'properties': {
        's': {
            'title': 'Seconds',
            'description': "Number of seconds from epoch. Can be negative "
                           "(represent time before epoch).",
            'type': 'integer'},
        'us': {
            'title': 'Microseconds',
            'description': "Microseconds within the second.",
            'type': 'integer',
            'minimum': 0,
            'maximum': 999999}}}

_event_id_schema = {
    'title': 'Event identifier',
    'type': 'object',
    'required': ['server', 'session', 'instance'],
    'properties': {
        'server': {
            'title': 'Server identifier',
            'description': 'Identifier of server that created the event.',
            'type': 'integer'},
        'session': {
            'title': 'Session identifier',
            'description': 'Identifier of session that created the event.',
            'type': 'integer'},
        'instance': {
            'title': 'Instance identifier',
            'description': 'Identifier of event as part of session.',
            'type': 'integer'}}}

_payload_schema = {
    'title': 'Payload',
    'oneOf': [
        {'title': 'Binary serialized payload',
         'type': 'object',
         'required': ['data_type', 'binary_type', 'data'],
         'properties': {
            'data_type': {
                'const': 'binary'},
            'binary_type': {
                'title': 'Binary type identifier',
                'type': 'string'},
            'data': {
                'title': 'Binary data',
                'description': "Hex encoded bytes.",
                'type': 'string'}}},
        {'title': 'JSON serialized payload',
         'type': 'object',
         'required': ['data_type', 'data'],
         'properties': {
            'data_type': {
                'const': 'json'},
            'data': {
                'title': 'JSON data'}}}]}

_event_schema = {
    'title': 'Event',
    'type': 'object',
    'required': ['id', 'type', 'timestamp', 'source_timestamp', 'payload'],
    'properties': {
        'id': _event_id_schema,
        'type': _event_type_schema,
        'timestamp': _timestamp_schema,
        'source_timestamp': {
            'oneOf': [
                {'type': 'null'},
                _timestamp_schema]},
        'payload': {
            'oneOf': [
                {'type': 'null'},
                _payload_schema]}}}

_query_latest_params_schema = {
    'title': 'Query latest parameters',
    'type': 'object',
    'properties': {
        'event_types': {
            'type': 'array',
            'items': _query_event_type_schema}}}

_query_timeseries_params_schema = {
    'title': 'Query timeseries parameters',
    'type': 'object',
    'properties': {
        'event_types': {
            'type': 'array',
            'items': _query_event_type_schema},
        't_from': _timestamp_schema,
        't_to': _timestamp_schema,
        'source_t_from': _timestamp_schema,
        'source_t_to': _timestamp_schema,
        'order': {
            'enum': ['descending', 'ascending'],
            'default': 'descending'},
        'order_by': {
            'enum': ['timestamp', 'sourceTimestamp'],
            'default': 'timestamp'},
        'max_results': {
            'type': 'integer'},
        'last_event_id': _event_id_schema}}

_query_server_params_schema = {
    'title': 'Query server parameters',
    'type': 'object',
    'required': ['server_id'],
    'properties': {
        'server_id': {
            'type': 'integer'},
        'persisted': {
            'type': 'boolean',
            'default': False},
        'max_results': {
            'type': 'integer'},
        'last_event_id': _event_id_schema}}

_query_result_schema = {
    'title': 'Query result',
    'type': 'object',
    'required': ['events', 'more_follows'],
    'properties': {
        'events': {
            'type': 'array',
            'items': _event_schema},
        'more_follows': {
            'type': 'boolean'}}}
