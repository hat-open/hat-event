import datetime
import struct
import typing

from hat import sbs


class Timestamp(typing.NamedTuple):
    s: int
    """seconds since 1970-01-01 (can be negative)"""
    us: int
    """microseconds added to timestamp seconds in range [0, 1e6)"""

    def __lt__(self, other):
        if not isinstance(other, Timestamp):
            return NotImplemented
        return self.s * 1000000 + self.us < other.s * 1000000 + other.us

    def __gt__(self, other):
        if not isinstance(other, Timestamp):
            return NotImplemented
        return self.s * 1000000 + self.us > other.s * 1000000 + other.us

    def __eq__(self, other):
        if not isinstance(other, Timestamp):
            return NotImplemented
        return self.s * 1000000 + self.us == other.s * 1000000 + other.us

    def __ne__(self, other):
        return not self == other

    def __le__(self, other):
        return self < other or self == other

    def __ge__(self, other):
        return self > other or self == other

    def __hash__(self):
        return self.s * 1000000 + self.us

    def add(self, s: float) -> 'Timestamp':
        """Create new timestamp by adding seconds to existing timestamp"""
        us = self.us + round((s - int(s)) * 1e6)
        s = self.s + int(s)
        return Timestamp(s=s + us // int(1e6),
                         us=us % int(1e6))


def now() -> Timestamp:
    """Create new timestamp representing current time"""
    return timestamp_from_datetime(
        datetime.datetime.now(datetime.timezone.utc))


def timestamp_to_bytes(t: Timestamp) -> bytes:
    """Convert timestamp to 12 byte representation

    Bytes [0, 8] are big endian unsigned `Timestamp.s` + 2^63 and
    bytes [9, 12] are big endian unsigned `Timestamp.us`.

    """
    return struct.pack(">QI", t.s + (1 << 63), t.us)


def timestamp_from_bytes(data: bytes) -> Timestamp:
    """Create new timestamp from 12 byte representation

    Bytes representation is same as defined for `timestamp_to_bytes` function.

    """
    s, us = struct.unpack(">QI", data)
    return Timestamp(s - (1 << 63), us)


def timestamp_to_float(t: Timestamp) -> float:
    """Convert timestamp to floating number of seconds since 1970-01-01 UTC

    For precise serialization see `timestamp_to_bytes`/`timestamp_from_bytes`.

    """
    return t.s + t.us * 1E-6


def timestamp_from_float(ts: float) -> Timestamp:
    """Create timestamp from floating number of seconds since 1970-01-01 UTC

    For precise serialization see `timestamp_to_bytes`/`timestamp_from_bytes`.

    """
    s = int(ts)
    if ts < 0:
        s = s - 1
    us = round((ts - s) * 1E6)
    if us == 1000000:
        return Timestamp(s + 1, 0)
    else:
        return Timestamp(s, us)


def timestamp_to_datetime(t: Timestamp) -> datetime.datetime:
    """Convert timestamp to datetime (representing utc time)

    For precise serialization see `timestamp_to_bytes`/`timestamp_from_bytes`.

    """
    try:
        dt_from_s = datetime.datetime.fromtimestamp(t.s, datetime.timezone.utc)
    except OSError:
        dt_from_s = (
            datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) +
            datetime.timedelta(seconds=t.s))
    return datetime.datetime(
        year=dt_from_s.year,
        month=dt_from_s.month,
        day=dt_from_s.day,
        hour=dt_from_s.hour,
        minute=dt_from_s.minute,
        second=dt_from_s.second,
        microsecond=t.us,
        tzinfo=datetime.timezone.utc)


def timestamp_from_datetime(dt: datetime.datetime) -> Timestamp:
    """Create new timestamp from datetime

    If `tzinfo` is not set, it is assumed that provided datetime represents
    utc time.

    For precise serialization see `timestamp_to_bytes`/`timestamp_from_bytes`.

    """
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    s = int(dt.timestamp())
    if dt.timestamp() < 0:
        s = s - 1
    return Timestamp(s=s, us=dt.microsecond)


def timestamp_to_sbs(t: Timestamp) -> sbs.Data:
    """Convert timestamp to SBS data"""
    return {'s': t.s, 'us': t.us}


def timestamp_from_sbs(data: sbs.Data) -> Timestamp:
    """Create new timestamp from SBS data"""
    return Timestamp(s=data['s'], us=data['us'])
