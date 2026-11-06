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
    mcp_server = mcp.Server(name="hat-event")

    mcp_server.add_tool(
        mcp.Tool(name='query_latest',
                 title=None,
                 description=None,
                 input_schema=_query_latest_params_schema,
                 output_schema=_query_result_schema,
                 call_cb=functools.partial(_process_query, client,
                                           rest.parse_latest_params)))

    mcp_server.add_tool(
        mcp.Tool(name='query_timeseries',
                 title=None,
                 description=None,
                 input_schema=_query_timeseries_params_schema,
                 output_schema=_query_result_schema,
                 call_cb=functools.partial(_process_query, client,
                                           rest.parse_timeseries_params)))

    mcp_server.add_tool(
        mcp.Tool(name='query_server',
                 title=None,
                 description=None,
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
    'type': 'array',
    'items': {
        'type': 'string'}}

_event_type_schema = {
    'type': 'array',
    'items': {
        'type': 'string'}}

_timestamp_schema = {
    'type': 'object',
    'required': ['s', 'us'],
    'properties': {
        's': {
            'type': 'integer'},
        'us': {
            'type': 'integer'}}}

_event_id_schema = {
    'type': 'object',
    'required': ['server', 'session', 'instance'],
    'properties': {
        'server': {
            'type': 'integer'},
        'session': {
            'type': 'integer'},
        'instance': {
            'type': 'integer'}}}

_payload_schema = {
    'oneOf': [
        {'type': 'object',
         'required': ['data_type', 'binary_type', 'data'],
         'properties': {
            'data_type': {
                'const': 'binary'},
            'binary_type': {
                'type': 'string'},
            'data': {
                'type': 'string'}}},
        {'type': 'object',
         'required': ['data_type', 'data'],
         'properties': {
            'data_type': {
                'const': 'json'}}}]}

_event_schema = {
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
    'type': 'object',
    'properties': {
        'event_types': {
            'type': 'array',
            'items': _query_event_type_schema}}}

_query_timeseries_params_schema = {
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
    'type': 'object',
    'required': ['events', 'more_follows'],
    'properties': {
        'events': {
            'type': 'array',
            'items': _event_schema},
        'more_follows': {
            'type': 'boolean'}}}
