import datetime
import itertools
import math

import pytest

from hat.event import common


@pytest.mark.parametrize("t1, t2", itertools.permutations([
    common.Timestamp(0, 0),
    common.Timestamp(0, 1),
    common.Timestamp(1, 0),
    common.Timestamp(1, 1),
    common.Timestamp(-1, 0),
    common.Timestamp(-1, 1),
    common.Timestamp(-1, 999_999)
], r=2))
def test_timestamp(t1, t2):
    us1 = 1_000_000 * t1.s + t1.us
    us2 = 1_000_000 * t2.s + t2.us

    assert (t1 == t2) is (us1 == us2)
    assert (t1 != t2) is (us1 != us2)
    assert (t1 < t2) is (us1 < us2)
    assert (t1 > t2) is (us1 > us2)
    assert (t1 <= t2) is (us1 <= us2)
    assert (t1 >= t2) is (us1 >= us2)
    assert (hash(t1) == hash(t2)) is (us1 == us2)

    with pytest.raises(TypeError):
        t1 < None

    with pytest.raises(TypeError):
        t1 > 'abc'

    assert t1 != 123


@pytest.mark.parametrize("t1, s, t2", [
    (common.Timestamp(0, 0), 0, common.Timestamp(0, 0)),
    (common.Timestamp(0, 0), 0.001, common.Timestamp(0, 1000)),
    (common.Timestamp(0, 0), -0.001, common.Timestamp(-1, 999000)),
    (common.Timestamp(0, 0), 1, common.Timestamp(1, 0)),
    (common.Timestamp(0, 0), 1.001, common.Timestamp(1, 1000)),
    (common.Timestamp(0, 0), -1.001, common.Timestamp(-2, 999000))
])
def test_timestamp_add(t1, s, t2):
    assert t1.add(s) == t2
    assert t2.add(-s) == t1


@pytest.mark.parametrize("t, t_bytes", [
    (common.Timestamp(0, 0),
     b'\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'),

    (common.Timestamp(2_145_916_800, 99_999),
     b'\x80\x00\x00\x00\x7f\xe8\x17\x80\x00\x01\x86\x9f'),

    (common.Timestamp(12_321, 99_999),
     b'\x80\x00\x00\x00\x00\x00\x30\x21\x00\x01\x86\x9f')
])
def test_timestamp_bytes(t, t_bytes):
    assert common.timestamp_to_bytes(t) == t_bytes
    assert common.timestamp_from_bytes(t_bytes) == t


@pytest.mark.parametrize("t, t_float", [
    (common.Timestamp(0, 0),
     0.0),

    (common.Timestamp(1, 0),
     1.0),

    (common.Timestamp(1_000, 10_000),
     1_000.01),

    (common.Timestamp(-473_385_600, 123),
     -473_385_600 + 123 * 1e-6),

    (common.Timestamp(473_385_600, 123),
     473_385_600 + 123 * 1e-6),

    (common.Timestamp(4_099_680_000, 123),
     4_099_680_000 + 123 * 1e-6),

    (common.Timestamp(409_968, 999_999),
     409_968 + 999_999 * 1e-6),

    (common.Timestamp(409_968, 999_999),
     409_968 + 9_999_994 * 1e-7),

    (common.Timestamp(409_969, 0),
     409_968 + 9_999_996 * 1e-7),

    (common.Timestamp(409_969, 0),
     409_968 + 9_999_999 * 1e-7),

    (common.Timestamp(-409_969, 1),
     -409_968 - 9_999_994 * 1e-7),

    (common.Timestamp(-409_969, 0),
     -409_968 - 9_999_996 * 1e-7),

    (common.Timestamp(-409_968, 0),
     -409_968 - 4 * 1e-7),

    (common.Timestamp(-409_969, 999_999),
     -409_968 - 6 * 1e-7)
])
def test_timestamp_float(t, t_float):
    assert math.isclose(common.timestamp_to_float(t), t_float, abs_tol=1e-9)
    assert common.timestamp_from_float(t_float) == t


@pytest.mark.parametrize("t, t_dt", [
    (common.Timestamp(-2_208_988_800, 999_999),
     datetime.datetime(1900, 1, 1, 0, 0, 0, 999_999,
                       tzinfo=datetime.timezone.utc)),

    (common.Timestamp(0, 0),
     datetime.datetime(1970, 1, 1, 0, 0, 0, 0,
                       tzinfo=datetime.timezone.utc)),

    (common.Timestamp(0, 123),
     datetime.datetime(1970, 1, 1, 0, 0, 0, 123,
                       tzinfo=datetime.timezone.utc)),

    (common.Timestamp(2_145_916_800, 99_999),
     datetime.datetime(2038, 1, 1, 0, 0, 0, 99_999,
                       tzinfo=datetime.timezone.utc)),

    (common.Timestamp(4_133_980_799, 1),
     datetime.datetime(2100, 12, 31, 23, 59, 59, 1,
                       tzinfo=datetime.timezone.utc))
])
def test_timestamp_datetime(t, t_dt):
    assert common.timestamp_to_datetime(t) == t_dt
    assert common.timestamp_from_datetime(t_dt) == t
    assert common.timestamp_from_datetime(t_dt.replace(tzinfo=None)) == t


@pytest.mark.parametrize("t", [
    common.Timestamp(-2_208_988_800, 999_999),
    common.Timestamp(0, 0),
    common.Timestamp(0, 123),
    common.Timestamp(2_145_916_800, 99_999),
    common.Timestamp(4_133_980_799, 1)
])
def test_timestamp_sbs(t):
    encoded = common.timestamp_to_sbs(t)
    decoded = common.timestamp_from_sbs(encoded)
    assert decoded == t


def test_now():
    previous_dt = None

    for _ in range(10):
        now_dt = common.timestamp_to_datetime(common.now())
        delta = datetime.datetime.now(datetime.timezone.utc) - now_dt

        assert delta < datetime.timedelta(seconds=1)
        if previous_dt is not None:
            assert now_dt >= previous_dt

        previous_dt = now_dt
