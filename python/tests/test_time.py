import pickle

import pytest
from raio import HOUR, MINUTE, MS, NS, SECOND, US, S, Time


def test_units_scale_by_a_thousand():
    assert US == 1000 * NS and MS == 1000 * US and S == 1000 * MS
    assert SECOND == S and MINUTE == 60 * S and HOUR == 60 * MINUTE


def test_times_add_subtract_and_compare():
    assert 2 * S + 500 * MS == Time(2_500_000_000)
    assert 2 * S - 500 * MS == 1500 * MS
    assert MS < S <= S and S > MS
    assert Time(5).nanos == 5 and Time(5).microstep == 0


def test_a_time_scales_by_a_float_and_divides():
    assert 1.5 * S == 1500 * MS
    assert S / 4 == 250 * MS
    assert S / MS == 1000.0
    assert (2 * S + MS) // S == 2
    with pytest.raises(ZeroDivisionError):
        S / Time.ZERO


def test_a_microstep_orders_timestamps_within_one_nanosecond():
    assert Time(5) < Time(5, 1) < Time(5, 2) < Time(6)
    assert Time(5, 2).microstep == 2


def test_the_sentinels_absorb():
    assert Time.NEVER < Time.ZERO < Time.FOREVER
    assert Time.FOREVER + S == Time.FOREVER and 3 * Time.NEVER == Time.NEVER


def test_a_time_prints_in_its_largest_whole_unit():
    assert str(1500 * MS) == "1500ms" and str(2 * S) == "2s" and str(Time(5, 1)) == "(5ns, 1)"
    assert repr(2 * S) == "Time(2s)"


def test_a_time_is_hashable_and_survives_pickling():
    assert {S: "second"}[1000 * MS] == "second"
    for time in (Time.ZERO, 3 * MS, Time(5, 2), Time.NEVER, Time.FOREVER):
        assert pickle.loads(pickle.dumps(time)) == time
