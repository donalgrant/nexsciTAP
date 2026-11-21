"""The count of what a client has been sent, and the cap on it (TAP/volume.py)."""
import io

import pytest

from TAP import volume


def test_sizes():
    assert volume.parse_size('20GB') == 20 * 1000 ** 3
    assert volume.parse_size(' 500 mb ') == 500 * 1000 ** 2
    assert volume.parse_size('1.5g') == 1500 * 1000 ** 2
    assert volume.parse_size('6000') == 6000
    for nothing in ('', None, '0', 'none'):
        assert volume.parse_size(nothing) == 0
    for bad in ('lots', '-1GB', '1 GB; rm', 'GB'):
        with pytest.raises(ValueError):
            volume.parse_size(bad)
    assert volume.show_size(20 * 1000 ** 3) == '20 GB'
    assert volume.show_size(1500 * 1000 ** 2) == '1.5 GB'
    assert volume.show_size(900) == '900 bytes'


def test_a_day_of_what_was_sent(tmp_path):
    w, hour = str(tmp_path), volume.HOUR
    now = 1_800_000_000.0
    assert volume.used(w, 'a', now) == (0, 0)

    volume.add(w, 'a', 10_000, now - 30 * hour)        # more than a day ago: not counted
    volume.add(w, 'a', 20_000, now - 23 * hour)
    volume.add(w, 'a', 5_000, now - 23 * hour)         # same hour, same file
    volume.add(w, 'a', 7_000, now)
    volume.add(w, 'a', 100, now)                       # too small to write down
    volume.add(w, 'b', 50_000, now)                    # someone else

    had, wait = volume.used(w, 'a', now)
    assert had == 32_000
    # The oldest hour that counts stops counting between one and two hours from now.
    assert hour <= wait <= 2 * hour + 1
    assert volume.used(w, 'b', now)[0] == 50_000

    # A day later only the last is left, and then nothing.
    assert volume.used(w, 'a', now + 23 * hour)[0] == 7_000
    assert volume.used(w, 'a', now + 26 * hour) == (0, 0)


def test_old_hours_are_removed(tmp_path):
    w, now = str(tmp_path), 1_800_000_000.0
    volume.add(w, 'a', 10_000, now - 72 * volume.HOUR)
    volume.add(w, 'a', 10_000, now)
    folder = tmp_path / 'TAP' / '.volume' / 'a'
    assert len(list(folder.iterdir())) == 2
    volume.used(w, 'a', now)
    assert len(list(folder.iterdir())) == 1


def test_a_client_name_cannot_leave_the_folder(tmp_path):
    volume.add(str(tmp_path), '../../x', 10_000)
    assert [p.name for p in (tmp_path / 'TAP' / '.volume').iterdir()] == ['.._.._x']
    assert not (tmp_path.parent / 'x').exists()


def test_a_counted_stream():
    under = io.StringIO()
    out = volume.Counted(under)
    out.write('abc')
    print('defg', file=out)
    out.writelines(['x', 'y'])
    out.flush()
    assert out.sent == 10 and under.getvalue() == 'abcdefg\nxy'
