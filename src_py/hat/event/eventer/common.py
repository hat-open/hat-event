from hat.event.common import *  # NOQA

from hat import sbs
from hat.drivers import chatter

from hat.event.common import sbs_repo


async def send_msg(conn: chatter.Connection,
                   msg_type: str,
                   msg_data: sbs.Data,
                   **kwargs
                   ) -> chatter.Conversation:
    msg = sbs_repo.encode(msg_type, msg_data)
    return await conn.send(chatter.Data(msg_type, msg), **kwargs)


async def receive_msg(conn: chatter.Connection
                      ) -> tuple[chatter.Msg, str, sbs.Data]:
    msg = await conn.receive()
    msg_data = sbs_repo.decode(msg.data.type, msg.data.data)
    return msg, msg.data.type, msg_data
