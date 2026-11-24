import json
from datetime import datetime

import pytest

from update_m3_schedule import SLOTS, TZ, updated_config


@pytest.mark.parametrize('hour,minute,noon,afternoon', [
    (11, 59, '2026-10-26', '2026-10-26'),
    (12, 0, '2026-10-27', '2026-10-26'),
    (16, 29, '2026-10-27', '2026-10-26'),
    (16, 30, '2026-10-27', '2026-10-27'),
    (23, 59, '2026-10-27', '2026-10-27'),
])
def test_config_update_preserves_existing_settings_and_avoids_new_slot_catchup(hour, minute, noon, afternoon):
    config = {'schedule_enabled': True, 'schedule_times': ['00:03', '04:00'],
              'schedule_start_dates': {'04:00': '2026-10-08'},
              'cookie_file': '/run/secrets/music_cookie.txt', 'threads': 4,
              'scan_mode': 'auto', 'unrelated': {'kept': True}}
    original = json.dumps(config, sort_keys=True)
    result = updated_config(config, datetime(2026, 10, 26, hour, minute, tzinfo=TZ))
    assert json.dumps(config, sort_keys=True) == original
    assert len(SLOTS) == 19 and SLOTS[0] == '12:00' and SLOTS[-1] == '16:30'
    assert all((int(b[:2]) * 60 + int(b[3:])) - (int(a[:2]) * 60 + int(a[3:])) == 15
               for a, b in zip(SLOTS, SLOTS[1:]))
    assert result['schedule_times'] == ['00:03', '04:00', *SLOTS]
    expected_starts = {}
    for slot in SLOTS:
        slot_hour, slot_minute = map(int, slot.split(':'))
        expected_starts[slot] = (noon if slot == '12:00' else afternoon if slot == '16:30'
                                 else '2026-10-27' if (hour, minute) >= (slot_hour, slot_minute)
                                 else '2026-10-26')
    assert result['schedule_start_dates'] == {'04:00': '2026-10-08', **expected_starts}
    assert result['schedule_modes'] == {slot: 'm3_intersection' for slot in SLOTS}
    assert result['schedule_end_dates'] == {slot: '2026-11-25' for slot in SLOTS}
    for key in ('schedule_enabled', 'threads', 'scan_mode', 'cookie_file', 'unrelated'):
        assert result[key] == config[key]
    # Reapplying later never moves an already configured activation date.
    assert updated_config(result, datetime(2026, 10, 29, 20, tzinfo=TZ)) == result


def test_config_update_handles_legacy_config_and_preserves_later_activation():
    result = updated_config({'schedule_enabled': False}, datetime(2026, 10, 26, 20, tzinfo=TZ))
    assert result['schedule_times'] == ['00:03', *SLOTS]
    assert result['schedule_enabled'] is False
    config = {'schedule_times': ['00:03', '04:00', '12:00'],
              'schedule_start_dates': {'12:00': '2026-11-01'},
              'schedule_modes': {'04:00': 'regular'}}
    result = updated_config(config, datetime(2026, 10, 26, 20, tzinfo=TZ))
    assert result['schedule_start_dates']['12:00'] == '2026-11-01'
    assert result['schedule_modes']['04:00'] == 'regular'


def test_expand_existing_two_slots_without_postponing_them_or_catching_up_new_past_slots():
    config = {'schedule_times': ['00:03', '04:00', '12:00', '16:30'],
              'schedule_start_dates': {'12:00': '2026-10-27', '16:30': '2026-10-27'},
              'schedule_modes': {s: 'm3_intersection' for s in ('12:00', '16:30')},
              'schedule_end_dates': {s: '2026-11-25' for s in ('12:00', '16:30')}}
    result = updated_config(config, datetime(2026, 10, 27, 14, 5, tzinfo=TZ))
    assert result['schedule_start_dates']['12:00'] == '2026-10-27'
    assert result['schedule_start_dates']['16:30'] == '2026-10-27'
    assert result['schedule_start_dates']['12:15'] == '2026-10-28'
    assert result['schedule_start_dates']['14:00'] == '2026-10-28'
    assert result['schedule_start_dates']['14:15'] == '2026-10-27'
    assert updated_config(result, datetime(2026, 10, 29, 20, tzinfo=TZ)) == result


def test_example_config_has_same_slots_and_modes():
    from pathlib import Path
    config = json.loads((Path(__file__).resolve().parents[1] / 'config.example.json').read_text())
    assert config['schedule_times'] == ['00:03', '04:00', *SLOTS]
    assert config['schedule_modes'] == {s: 'm3_intersection' for s in SLOTS}
    assert config['schedule_end_dates'] == {s: '2026-11-25' for s in SLOTS}
