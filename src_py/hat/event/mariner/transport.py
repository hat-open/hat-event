import itertools
import math

from hat import aio
from hat import json
from hat.drivers import tcp

from hat.event.mariner import common
from hat.event.mariner import encoder


class Transport(aio.Resource):

    def __init__(self, conn: tcp.Connection):
        self._conn = conn

    @property
    def async_group(self) -> aio.Group:
        return self._conn.async_group

    async def drain(self):
        await self._conn.drain()

    async def send(self, msg: common.Msg):
        msg_json = encoder.encode_msg(msg)
        msg_bytes = json.encode(msg_json).encode('utf-8')
        msg_len = len(msg_bytes)
        len_size = math.ceil(msg_len.bit_length() / 8)

        if len_size < 1 or len_size > 8:
            raise ValueError('unsupported msg size')

        data = bytes(itertools.chain([len_size],
                                     msg_len.to_bytes(len_size, 'big'),
                                     msg_bytes))
        await self._conn.write(data)

    async def receive(self) -> common.Msg:
        len_size_bytes = await self._conn.readexactly(1)
        len_size = len_size_bytes[0]

        if len_size < 1 or len_size > 8:
            raise ValueError('unsupported msg size')

        msg_len_bytes = await self._conn.readexactly(len_size)
        msg_len = int.from_bytes(msg_len_bytes, 'big')

        msg_bytes = await self._conn.readexactly(msg_len)
        msg_str = str(msg_bytes, 'utf-8')
        msg_json = json.decode(msg_str)

        return encoder.decode_msg(msg_json)
