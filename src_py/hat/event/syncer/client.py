import typing
import logging

from hat import aio
from hat import chatter

from hat.event.syncer import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

SyncedCb = aio.AsyncCallable[[], None]
"""Synced callback"""

EventsCb = aio.AsyncCallable[[typing.List[common.Event]], None]
"""Events callback"""


async def connect(address: str,
                  client_name: str,
                  last_event_id: common.EventId,
                  synced_cb: typing.Optional[SyncedCb] = None,
                  events_cb: typing.Optional[EventsCb] = None
                  ) -> 'Client':
    """Connect to remote syncer server"""
    client = Client()
    client._synced_cb = synced_cb
    client._events_cb = events_cb

    client._conn = await chatter.connect(common.sbs_repo, address)
    mlog.debug("connected to %s", address)

    try:
        req = common.SyncerReq(last_event_id=last_event_id,
                               client_name=client_name)

        mlog.debug("sending %s", req)
        msg_data = chatter.Data(module='HatSyncer',
                                type='MsgReq',
                                data=common.syncer_req_to_sbs(req))
        client._conn.send(msg_data)

        client.async_group.spawn(client._receive_loop)

    except BaseException:
        await aio.uncancellable(client.async_close())
        raise

    return client


class Client(aio.Resource):
    """Syncer client"""

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._conn.async_group

    async def _receive_loop(self):
        mlog.debug("staring receive loop")
        try:
            while True:
                mlog.debug("waiting for incoming message")
                msg = await self._conn.receive()
                msg_type = msg.data.module, msg.data.type

                if msg_type == ('HatSyncer', 'MsgEvents'):
                    mlog.debug("received events")
                    events = [common.event_from_sbs(i)
                              for i in msg.data.data]

                    if self._events_cb:
                        await aio.call(self._events_cb, events)

                elif msg_type == ('HatSyncer', 'MsgSynced'):
                    mlog.debug("received synced")

                    if self._synced_cb:
                        await aio.call(self._synced_cb)

                elif msg_type == ('HatSyncer', 'MsgFlushReq'):
                    mlog.debug("received flush request")
                    self._conn.send(chatter.Data(module='HatSyncer',
                                                 type='MsgFlushRes',
                                                 data=None),
                                    conv=msg.conv)

                else:
                    raise Exception("unsupported message type")

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("receive loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping receive loop")
            self.close()
