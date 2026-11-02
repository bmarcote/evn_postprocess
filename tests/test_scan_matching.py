"""Tests for matching MS scans to VEX scans by time, and for the PolConvert plot renaming/cleanup."""
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from evn_postprocess import process

T0 = datetime(2026, 6, 8, 21, 0, 0)


def vex_scan(number: int, start_min: float, duration_s: int = 120):
    return SimpleNamespace(scanno=f"No{number:04d}", starttime=T0 + timedelta(minutes=start_min), duration_s=duration_s)


# VEX scans 10, 11, 12, each 2 min long, starting every 3 min.
VEX = [vex_scan(10, 0), vex_scan(11, 3), vex_scan(12, 6)]


def test_offset_numbering_is_matched_by_time():
    """MS scans 1, 2, 3 (numbered from 1) are VEX scans 10, 11, 12."""
    midtimes = {1: T0 + timedelta(minutes=1), 2: T0 + timedelta(minutes=4), 3: T0 + timedelta(minutes=7)}
    assert process._match_scans_by_time(midtimes, VEX) == {1: 10, 2: 11, 3: 12}


def test_split_scan_and_unmatched_scan():
    """Two MS scans inside VEX scan 11 both map to 11; a MS scan in a gap between scans maps to none."""
    midtimes = {1: T0 + timedelta(minutes=3, seconds=20), 2: T0 + timedelta(minutes=4, seconds=40),
                3: T0 + timedelta(minutes=5, seconds=30)}
    assert process._match_scans_by_time(midtimes, VEX) == {1: 11, 2: 11}


def test_map_falls_back_to_equal_numbers_when_ms_times_unreadable(monkeypatch):
    def broken(ms):
        raise RuntimeError("no such MS")
    monkeypatch.setattr(process.plotting, 'scan_midtimes', broken)
    exp = SimpleNamespace(scans=VEX)
    assert process._ms_to_vex_scan_map(exp, "x.ms", [10, 12, 99]) == {10: 10, 12: 12}


def test_map_uses_time_when_available(monkeypatch):
    monkeypatch.setattr(process.plotting, 'scan_midtimes', lambda ms: {1: T0 + timedelta(minutes=7)})
    assert process._ms_to_vex_scan_map(SimpleNamespace(scans=VEX), "x.ms", [1]) == {1: 12}


def test_rename_split_scans_get_parts_and_unmatched_is_removed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for n in (1, 2, 3, 4):
        Path(f"exp-pconv-cross-scan{n}.ps").write_text(str(n))
    plots = [f"exp-pconv-cross-scan{n}.ps" for n in (1, 2, 3, 4)]
    renamed = process._rename_pconv_plots(plots, "exp", {1: 10, 2: 11, 3: 11})
    assert renamed == {"exp-pconv-cross-scan1.ps": "exp-cross-scan10.ps",
                       "exp-pconv-cross-scan2.ps": "exp-cross-scan11.ps",
                       "exp-pconv-cross-scan3.ps": "exp-cross-scan11-part2.ps"}
    assert sorted(p.name for p in tmp_path.glob("*.ps")) == ["exp-cross-scan10.ps", "exp-cross-scan11-part2.ps",
                                                            "exp-cross-scan11.ps"]


def test_rename_orders_by_scan_number_not_by_string(tmp_path, monkeypatch):
    """MS scan 9 comes before MS scan 10 when both fall in the same VEX scan."""
    monkeypatch.chdir(tmp_path)
    for n in (9, 10):
        Path(f"exp-pconv-cross-scan{n}.ps").write_text(str(n))
    process._rename_pconv_plots(["exp-pconv-cross-scan10.ps", "exp-pconv-cross-scan9.ps"], "exp", {9: 5, 10: 5})
    assert Path("exp-cross-scan5.ps").read_text() == "9"
    assert Path("exp-cross-scan5-part2.ps").read_text() == "10"


def test_remove_cross_plots_includes_gzipped_and_orphan_pngs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    plots = tmp_path / "plots"
    plots.mkdir()
    for name in ("exp-cross-scan1.ps", "exp-cross-scan2.ps.gz", "exp-auto-scan1.ps"):
        Path(name).write_text("x")
    for name in ("exp-cross-scan1-page001.png", "exp-cross-scan7-page001.png", "exp-auto-scan1-page001.png"):
        (plots / name).write_text("x")
    process._remove_cross_plots("exp", plots)
    assert sorted(p.name for p in tmp_path.glob("*.ps*")) == ["exp-auto-scan1.ps"]
    assert [p.name for p in plots.iterdir()] == ["exp-auto-scan1-page001.png"]


def test_map_falls_back_when_no_ms_scan_matches_in_time(monkeypatch):
    """E.g. VEX scans with wrong times or duration 0: an empty time map must not empty the scan list."""
    monkeypatch.setattr(process.plotting, 'scan_midtimes', lambda ms: {10: T0 + timedelta(days=3)})
    assert process._ms_to_vex_scan_map(SimpleNamespace(scans=VEX), "x.ms", [10]) == {10: 10}
