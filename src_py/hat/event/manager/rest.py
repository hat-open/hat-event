import collections
import functools

import aiohttp.web

from hat import aio
from hat import json
from hat import juggler
from hat.drivers import net

from hat.event import eventer
from hat.event.manager import common


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
    query_routes = [
        aiohttp.web.get(
            f'/query/{path}',
            functools.partial(_process_get_query, parse_params, client))
        for path, parse_params in [('latest', _parse_latest_params),
                                   ('timeseries', _parse_timeseries_params),
                                   ('server', _parse_server_params)]]

    return await juggler.listen(
        host=http_addr.host,
        port=http_addr.port,
        ws_path=None,
        index_path=None,
        additional_routes=query_routes)


async def _process_get_query(parse_params, client, req):
    try:
        body = await req.json()
        params = parse_params(body)

    except Exception as e:
        raise aiohttp.web.HTTPBadRequest(text=f"error reading params: {e}")

    try:
        result = await client.query(params)

        return aiohttp.web.Response(
            content_type='application/json',
            text=json.encode(common.query_result_to_json(result)))

    except Exception as e:
        raise aiohttp.web.HTTPInternalServerError(text=f"query error: {e}")


def _parse_latest_params(params):
    event_types = (_parse_event_types(params['event_types'])
                   if 'event_types' in params else None)

    return common.QueryLatestParams(event_types=event_types)


def _parse_timeseries_params(params):
    event_types = (_parse_event_types(params['event_types'])
                   if 'event_types' in params else None)
    t_from = _parse_timestamp(params['t_from']) if 't_from' in params else None
    t_to = _parse_timestamp(params['t_to']) if 't_to' in params else None
    source_t_from = (_parse_timestamp(params['source_t_from'])
                     if 'source_t_from' in params else None)
    source_t_to = (_parse_timestamp(params['source_t_to'])
                   if 'source_t_to' in params else None)
    order = common.Order(params.get('order', 'descending'))
    order_by = common.OrderBy(params.get('order_by', 'timestamp'))
    max_results = (_parse_max_results(params['max_results'])
                   if 'max_results' in params else None)
    last_event_id = (_parse_event_id(params['last_event_id'])
                     if 'last_event_id' in params else None)

    return common.QueryTimeseriesParams(event_types=event_types,
                                        t_from=t_from,
                                        t_to=t_to,
                                        source_t_from=source_t_from,
                                        source_t_to=source_t_to,
                                        order=order,
                                        order_by=order_by,
                                        max_results=max_results,
                                        last_event_id=last_event_id)


def _parse_server_params(params):
    server_id = _parse_server_id(params['server_id'])
    persisted = (_parse_persisted(params['persisted'])
                 if 'persisted' in params else False)
    max_results = (_parse_max_results(params['max_results'])
                   if 'max_results' in params else None)
    last_event_id = (_parse_event_id(params['last_event_id'])
                     if 'last_event_id' in params else None)

    return common.QueryServerParams(server_id=server_id,
                                    persisted=persisted,
                                    max_results=max_results,
                                    last_event_id=last_event_id)


def _parse_event_types(event_types):
    result = collections.deque()

    for event_type in event_types:
        event_type = tuple(event_type)

        for segment in event_type:
            if not isinstance(segment, str):
                raise Exception('invalid event type')

        result.append(event_type)

    return result


def _parse_server_id(server_id):
    if not isinstance(server_id, int):
        raise Exception('invalid server id')

    return int(server_id)


def _parse_max_results(max_results):
    if not isinstance(max_results, int):
        raise Exception('invalid max results')

    return int(max_results)


def _parse_persisted(persisted):
    if not isinstance(persisted, bool):
        raise Exception('invalid persisted')

    return persisted


def _parse_timestamp(t):
    if not isinstance(t['s'], int):
        raise Exception('invalid timestamp seconds')

    if not isinstance(t['us'], int) or not (0 <= t['us'] < 1_000_000):
        raise Exception('invalid timestamp micro seconds')

    return common.Timestamp(s=int(t['s']),
                            us=int(t['us']))


def _parse_event_id(event_id):
    if (not isinstance(event_id['server'], int) or
            not isinstance(event_id['session'], int) or
            not isinstance(event_id['instance'], int)):
        raise Exception('invalid event id')

    return common.EventId(server=int(event_id['server']),
                          session=int(event_id['session']),
                          instance=int(event_id['instance']))


async def _bind_resource(async_group, resource):
    try:
        async_group.spawn(aio.call_on_cancel, resource.async_close)
        async_group.spawn(aio.call_on_done, resource.wait_closing(),
                          async_group.close)

    except Exception:
        await aio.uncancellable(resource.async_close())
        raise
