"""Tests for the .flag fallback that fills in missing/empty per-antenna .uvflgfs files."""
from pathlib import Path
from types import SimpleNamespace
from evn_postprocess import pipeline

FLAG = ("antenna='EF' timerang=1,0,0,0,1,1,0,0\n"
        + "antenna='WB' timerang=1,2,0,0,1,3,0,0\n"
        + "antenna='EF' timerang=1,5,0,0,1,6,0,0")


def make_exp(observed: list[str]):
    return SimpleNamespace(expname="TESTEXP", antennas=SimpleNamespace(observed=observed))


def test_real_uvflgfs_kept_empty_and_missing_use_flag(tmp_path: Path):
    (tmp_path / "testexp.flag").write_text(FLAG)
    (tmp_path / "testexpef.uvflgfs").write_text("antenna=5 timerang=9,9,9,9,9,9,9,9\n")
    (tmp_path / "testexpwb.uvflgfs").write_text("")
    result = pipeline.supplement_uvflgfs_from_flag(make_exp(["Ef", "Wb", "Mc"]), tmp_path)
    assert result == {'log': ['EF'], 'flag': ['WB'], 'none': ['MC']}
    assert (tmp_path / "testexpef.uvflgfs").read_text() == "antenna=5 timerang=9,9,9,9,9,9,9,9\n"
    wb = (tmp_path / "testexpwb.uvflgfs").read_text().splitlines()
    assert wb[1:] == ["antenna='WB' timerang=1,2,0,0,1,3,0,0"]
    assert not (tmp_path / "testexpmc.uvflgfs").exists()


def test_flag_lines_of_multiple_entries_and_no_trailing_newline(tmp_path: Path):
    (tmp_path / "testexp.flag").write_text(FLAG)
    result = pipeline.supplement_uvflgfs_from_flag(make_exp(["Ef"]), tmp_path)
    assert result['flag'] == ['EF']
    assert len((tmp_path / "testexpef.uvflgfs").read_text().splitlines()) == 3  # comment + 2 lines


def test_no_flag_file_leaves_everything_untouched(tmp_path: Path):
    result = pipeline.supplement_uvflgfs_from_flag(make_exp(["Ef"]), tmp_path)
    assert result == {'log': [], 'flag': [], 'none': ['EF']}
    assert not list(tmp_path.glob("*.uvflgfs"))


def test_comment_only_uvflgfs_counts_as_empty(tmp_path: Path):
    (tmp_path / "testexp.flag").write_text(FLAG)
    (tmp_path / "testexpwb.uvflgfs").write_text("! nothing to flag\n")
    assert pipeline.supplement_uvflgfs_from_flag(make_exp(["Wb"]), tmp_path)['flag'] == ['WB']
