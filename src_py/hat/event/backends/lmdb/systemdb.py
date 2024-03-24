import typing

import lmdb

from hat.event.backends.lmdb import common
from hat.event.backends.lmdb import environment


class Changes(typing.NamedTuple):
    last_event_ids: dict[common.ServerId, common.EventId]
    last_timestamps: dict[common.ServerId, common.Timestamp]


def ext_create(env: environment.Environment,
               txn: lmdb.Transaction,
               version: str,
               identifier: str | None
               ) -> 'SystemDb':
    db = SystemDb()
    db._env = env
    db._changes = Changes({}, {})

    _ext_validate_settings(env, txn, version, identifier)

    db._last_event_ids = dict(
        env.ext_read(txn, common.DbType.SYSTEM_LAST_EVENT_ID))

    db._last_timestamps = dict(
        env.ext_read(txn, common.DbType.SYSTEM_LAST_TIMESTAMP))

    return db


class SystemDb:

    def get_last_event_id(self,
                          server_id: common.ServerId
                          ) -> common.EventId:
        if server_id in self._last_event_ids:
            return self._last_event_ids[server_id]

        return common.EventId(server=server_id,
                              session=0,
                              instance=0)

    def set_last_event_id(self, event_id: common.EventId):
        self._changes.last_event_ids[event_id.server] = event_id
        self._last_event_ids[event_id.server] = event_id

    def get_last_timestamp(self,
                           server_id: common.ServerId
                           ) -> common.Timestamp:
        if server_id in self._last_timestamps:
            return self._last_timestamps[server_id]

        return common.min_timestamp

    def set_last_timestamp(self,
                           server_id: common.ServerId,
                           timestamp: common.Timestamp):
        self._changes.last_timestamps[server_id] = timestamp
        self._last_timestamps[server_id] = timestamp

    def create_changes(self) -> Changes:
        changes, self._changes = self._changes, Changes({}, {})
        return changes

    def ext_write(self,
                  txn: lmdb.Transaction,
                  changes: Changes):
        self._env.ext_write(txn, common.DbType.SYSTEM_LAST_EVENT_ID,
                            changes.last_event_ids.items())

        self._env.ext_write(txn, common.DbType.SYSTEM_LAST_TIMESTAMP,
                            changes.last_timestamps.items())


def _ext_validate_settings(env, txn, version, identifier):
    settings = dict(env.ext_read(txn, common.DbType.SYSTEM_SETTINGS))

    if common.SettingsId.VERSION not in settings:
        settings[common.SettingsId.VERSION] = version

    elif settings[common.SettingsId.VERSION] != version:
        raise Exception('invalid version')

    else:
        settings.pop(common.SettingsId.VERSION)

    if identifier is None:
        settings.pop(common.SettingsId.IDENTIFIER, None)

    elif common.SettingsId.IDENTIFIER not in settings:
        settings[common.SettingsId.IDENTIFIER] = identifier

    elif settings[common.SettingsId.IDENTIFIER] != identifier:
        raise Exception('invalid identifier')

    else:
        settings.pop(common.SettingsId.IDENTIFIER)

    env.ext_write(txn, common.DbType.SYSTEM_SETTINGS, settings.items())
