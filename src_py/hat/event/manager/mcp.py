import functools

import aiohttp.web

from hat import aio
from hat import juggler
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


class _McpError(Exception):

    def __init__(self,
                 http_code: int,
                 jsonrpc_code: int,
                 message: str):
        super().__init__(message)

        self._http_code = http_code
        self._jsonrpc_code = jsonrpc_code
        self._message = message

    @property
    def http_code(self) -> int:
        return self._http_code

    @property
    def jsonrpc_code(self) -> int:
        return self._jsonrpc_code

    @property
    def message(self) -> int:
        return self._message


async def _create_web_server(http_addr, client):
    return await juggler.listen(
        host=http_addr.host,
        port=http_addr.port,
        ws_path=None,
        index_path=None,
        additional_routes=[
            aiohttp.web.post('/', functools.partial(_process, client))])


async def _process(client, req):
    jsonrpc_id = None

    try:
        try:
            body = await req.json()

        except Exception:
            raise _McpError(http_code=400,
                            jsonrpc_code=-32700,
                            message='Parse error')

        if (not isinstance(body, dict) or
                body.get('jsonrpc') != '2.0' or
                not isinstance(body.get('id'), (str, int)) or
                not isinstance(body.get('method'), str)):
            raise _McpError(http_code=400,
                            jsonrpc_code=-32600,
                            message='Invalid request')

        jsonrpc_id = body['id']
        params = body.get('params')

        try:
            if body['method'] == 'server/discover':
                result = _process_server_discover()

            elif body['method'] == 'tools/list':
                result = _process_tools_list()

            elif body['method'] == 'tools/call':
                result = await _process_tools_call(client, params)

            else:
                raise _McpError(http_code=404,
                                jsonrpc_code=-32601,
                                message='Method not found')

        except _McpError:
            raise

        except Exception as e:
            raise _McpError(http_code=500,
                            jsonrpc_code=-32603,
                            message=f"Internal error: {e}")

        data = {'jsonrpc': '2.0',
                'id': jsonrpc_id,
                'result': {
                    'resultType': 'complete',
                    **result}}

        return aiohttp.web.json_response(data)

    except _McpError as e:
        data = {'jsonrpc': '2.0',
                'error': {'code': e.jsonrpc_code,
                          'message': e.message}}

        if jsonrpc_id is not None:
            data['id'] = jsonrpc_id

        return aiohttp.web.json_response(data, status=e.http_code)

    except Exception:
        raise aiohttp.web.HTTPInternalServerError()


def _process_server_discover():
    return {'supportedVersions': ['2026-07-28'],
            'capabilities': {'tools': {'listChanged': False}},
            'ttlMs': _cache_ttl_ms,
            'cacheScope': 'public'}


def _process_tools_list():
    return {'tools': [{'name': 'query_latest',
                       'inputSchema': _query_latest_params_schema,
                       'outputSchema': _query_result_schema},
                      {'name': 'query_timeseries',
                       'inputSchema': _query_timeseries_params_schema,
                       'outputSchema': _query_result_schema},
                      {'name': 'query_server',
                       'inputSchema': _query_server_params_schema,
                       'outputSchema': _query_result_schema},],
            'ttlMs': _cache_ttl_ms,
            'cacheScope': 'public'}


async def _process_tools_call(client, params):
    try:
        name = params['name']
        assert isinstance(name, str)

        args = params['arguments']
        assert isinstance(args, dict)

    except Exception:
        raise _McpError(http_code=400,
                        jsonrpc_code=-32602,
                        message='Invalid params')

    try:
        if name == 'query_latest':
            result = await _process_tools_call_query(
                client, rest.parse_latest_params, args)

        elif name == 'query_timeseries':
            result = await _process_tools_call_query(
                client, rest.parse_timeseries_params, args)

        elif name == 'query_server':
            result = await _process_tools_call_query(
                client, rest.parse_server_params, args)

        else:
            raise _McpError(http_code=400,
                            jsonrpc_code=-32602,
                            message='Invalid params')

        return {'content': [],
                'structuredContent': result}

    except _McpError:
        raise

    except Exception as e:
        return {'content': [{'type': 'text',
                             'text': str(e)}],
                'isError': True}


async def _process_tools_call_query(client, parse_params, args):
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


_cache_ttl_ms = 24 * 60 * 60 * 1000

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
