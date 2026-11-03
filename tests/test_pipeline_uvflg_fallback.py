"""Tests for the .flag fallback that fills in missing/empty per-antenna .uvflgfs files and builds the .uvflg."""
from pathlib import Path
from types import SimpleNamespace
from evn_postprocess import pipeline

# Header and lines exactly as vlbeer writes an {exp}.flag file (mixed-case ant_name).
VLBEER_FLAG = """!  Apriori edit data for TESTEXP
!  The format is appropriate for UVFLG in AIPS.
opcode = 'FLAG'
dtimrang = 1  timeoff = 0
ant_name='Ef' timerang=159,21,00,00, 159,21,00,10  reason='Slewing expected.' /
ant_name='Wb' timerang=159,21,02,00, 159,21,02,05  reason='Slewing expected.' /
ant_name='Ef' timerang=159,21,05,00, 159,21,05,20  reason='Slewing expected.' /"""

# Header as uvflg.pl writes it at the top of a .uvflgfs made from a station .log.
UVFLG_PL_HEADER = ("! The following is flagging information for WB in experiment ./testexp,\n"
                   "! extracted from ./testexpwb.log by uvflg.pl version \"7 July 2003\".\n"
                   "opcode='FLAG'\n"
                   "dtimrang = 1   timeoff=0\n")


def make_exp(observed: list[str], tmp_path: Path | None = None):
    exp = SimpleNamespace(expname="TESTEXP", antennas=SimpleNamespace(observed=observed), correlator_passes=[])
    if tmp_path is not None:
        exp.dirs = SimpleNamespace(pipe_temp=tmp_path)
    return exp


def test_vlbeer_mixed_case_ant_name_is_matched(tmp_path: Path):
    """The vlbeer format (ant_name='Ef') must be found for antenna EF (bug: only antenna='EF' matched)."""
    (tmp_path / "testexp.flag").write_text(VLBEER_FLAG)
    result = pipeline.supplement_uvflgfs_from_flag(make_exp(["Ef", "Wb", "Mc"]), tmp_path)
    assert result == {'log': [], 'flag': ['EF', 'WB'], 'none': ['MC']}
    ef = (tmp_path / "testexpef.uvflgfs").read_text().splitlines()
    assert ef[1:3] == ["opcode='FLAG'", "dtimrang = 1   timeoff=0"]
    assert ef[3:] == ["ant_name='EF' timerang=159,21,00,00, 159,21,00,10  reason='Slewing expected.' /",
                      "ant_name='EF' timerang=159,21,05,00, 159,21,05,20  reason='Slewing expected.' /"]


def test_real_uvflgfs_kept_empty_and_missing_use_flag(tmp_path: Path):
    (tmp_path / "testexp.flag").write_text(VLBEER_FLAG)
    real = UVFLG_PL_HEADER.replace('WB', 'EF') + "ant_name='EF' timerang=159,20,35,50, 159,20,36,20 /\n"
    (tmp_path / "testexpef.uvflgfs").write_text(real)
    (tmp_path / "testexpwb.uvflgfs").write_text("")
    result = pipeline.supplement_uvflgfs_from_flag(make_exp(["Ef", "Wb", "Mc"]), tmp_path)
    assert result == {'log': ['EF'], 'flag': ['WB'], 'none': ['MC']}
    assert (tmp_path / "testexpef.uvflgfs").read_text() == real
    assert not (tmp_path / "testexpmc.uvflgfs").exists()


def test_legacy_antenna_format_still_supported(tmp_path: Path):
    (tmp_path / "testexp.flag").write_text("antenna='EF' timerang=1,0,0,0,1,1,0,0\nantenna='EF' timerang=1,5,0,0,1,6,0,0")
    assert pipeline.supplement_uvflgfs_from_flag(make_exp(["Ef"]), tmp_path)['flag'] == ['EF']
    assert len((tmp_path / "testexpef.uvflgfs").read_text().splitlines()) == 5  # comment + 2 header + 2 lines


def test_header_only_uvflgfs_counts_as_empty(tmp_path: Path):
    """A uvflg.pl file with only its opcode/dtimrang header has no flagging and must be filled."""
    (tmp_path / "testexp.flag").write_text(VLBEER_FLAG)
    (tmp_path / "testexpwb.uvflgfs").write_text(UVFLG_PL_HEADER)
    assert pipeline.supplement_uvflgfs_from_flag(make_exp(["Wb"]), tmp_path)['flag'] == ['WB']


def test_comment_only_uvflgfs_counts_as_empty(tmp_path: Path):
    (tmp_path / "testexp.flag").write_text(VLBEER_FLAG)
    (tmp_path / "testexpwb.uvflgfs").write_text("! nothing to flag\n")
    assert pipeline.supplement_uvflgfs_from_flag(make_exp(["Wb"]), tmp_path)['flag'] == ['WB']


def test_no_flag_file_leaves_everything_untouched(tmp_path: Path):
    result = pipeline.supplement_uvflgfs_from_flag(make_exp(["Ef"]), tmp_path)
    assert result == {'log': [], 'flag': [], 'none': ['EF']}
    assert not list(tmp_path.glob("*.uvflgfs"))


def test_suffixed_flag_file_is_found(tmp_path: Path):
    (tmp_path / "testexp_1.flag").write_text(VLBEER_FLAG)
    assert pipeline.find_flag_file(tmp_path, "TESTEXP") == tmp_path / "testexp_1.flag"
    (tmp_path / "testexp.flag").write_text(VLBEER_FLAG)
    assert pipeline.find_flag_file(tmp_path, "TESTEXP") == tmp_path / "testexp.flag"


def test_concatenation_adds_missing_newlines(tmp_path: Path):
    (tmp_path / "a.uvflgfs").write_text("ant_name='CM' x /")
    (tmp_path / "b.uvflgfs").write_text("ant_name='EF' y /\n")
    out = tmp_path / "x.uvflg"
    assert len(pipeline.concatenate_uvflgfs(tmp_path, out)) == 2
    assert out.read_text() == "ant_name='CM' x /\nant_name='EF' y /\n"


def test_create_uvflg_flag_only_every_block_has_its_header(tmp_path: Path):
    """Flag-only mode (no .log): a .uvflg is built and each antenna block starts with opcode/dtimrang."""
    (tmp_path / "testexp.flag").write_text(VLBEER_FLAG)
    assert pipeline.create_uvflg(make_exp(["Ef", "Wb"], tmp_path)) is True
    lines = (tmp_path / "testexp.uvflg").read_text().splitlines()
    assert lines.count("opcode='FLAG'") == 2
    assert sum(1 for line in lines if line.startswith("ant_name=")) == 3


def test_create_uvflg_returns_false_without_any_flagging(tmp_path: Path):
    """A .flag file with nothing for the observed antennas yields no .uvflgfs: False, not an exception."""
    (tmp_path / "testexp.flag").write_text(VLBEER_FLAG)
    assert pipeline.create_uvflg(make_exp(["Mc"], tmp_path)) is False
    assert not (tmp_path / "testexp.uvflg").exists()


def test_create_uvflg_returns_false_without_logs_or_flag(tmp_path: Path):
    assert pipeline.create_uvflg(make_exp(["Ef"], tmp_path)) is False


def test_create_uvflg_with_relative_pipe_temp(tmp_path: Path, monkeypatch):
    """Dirs are stored relative to the experiment folder; create_uvflg chdirs into pipe_temp and must still work."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "antenna_files").mkdir()
    (tmp_path / "antenna_files" / "testexp.flag").write_text(VLBEER_FLAG)
    exp = make_exp(["Ef", "Wb"])
    exp.dirs = SimpleNamespace(pipe_temp=Path("antenna_files"))
    assert pipeline.create_uvflg(exp) is True
    assert (tmp_path / "antenna_files" / "testexp.uvflg").exists()
    assert Path.cwd() == tmp_path


def test_only_the_antenna_name_is_capitalised(tmp_path: Path):
    """A station code that also occurs inside the keyword (e.g. 'an' in ant_name) must not touch the keyword."""
    (tmp_path / "testexp.flag").write_text("ant_name='an' timerang=1,0,0,0,1,1,0,0 /\n")
    pipeline.supplement_uvflgfs_from_flag(make_exp(["An"]), tmp_path)
    assert (tmp_path / "testexpan.uvflgfs").read_text().splitlines()[-1] == "ant_name='AN' timerang=1,0,0,0,1,1,0,0 /"


def test_fallback_file_is_refreshed_not_taken_as_real(tmp_path: Path):
    (tmp_path / "testexp.flag").write_text(VLBEER_FLAG)
    pipeline.supplement_uvflgfs_from_flag(make_exp(["Wb"]), tmp_path)
    assert pipeline.supplement_uvflgfs_from_flag(make_exp(["Wb"]), tmp_path)['flag'] == ['WB']


def test_aips_antennas_abbreviations_count_as_flags(tmp_path: Path):
    """Hand-written UVFLG lines (ANTE=, antennas=) are real flagging and must not be overwritten."""
    (tmp_path / "testexp.flag").write_text(VLBEER_FLAG)
    (tmp_path / "testexpwb.uvflgfs").write_text("ANTE=2,0 timerang=1,0,0,0,1,1,0,0 /\n")
    assert pipeline.supplement_uvflgfs_from_flag(make_exp(["Wb"]), tmp_path)['log'] == ['WB']


def test_suffix_glob_does_not_pick_another_experiment(tmp_path: Path):
    (tmp_path / "testexpb.flag").write_text(VLBEER_FLAG)  # a different experiment, TESTEXPB
    assert pipeline.find_flag_file(tmp_path, "TESTEXP") is None


def test_undecodable_bytes_do_not_raise(tmp_path: Path):
    (tmp_path / "testexp.flag").write_bytes(VLBEER_FLAG.encode() + b"\n! \xff\xfe bad\n")
    assert pipeline.create_uvflg(make_exp(["Ef"], tmp_path)) is True


def test_existing_uvflg_kept_but_unflagged_antennas_reported(tmp_path: Path):
    (tmp_path / "testexp.uvflg").write_text("ant_name='EF' timerang=1,0,0,0,1,1,0,0 /\n")
    exp = make_exp(["Ef", "Cm"], tmp_path)
    assert pipeline.create_uvflg(exp) is True
    assert pipeline._warn_unflagged_antennas(exp, tmp_path / "testexp.uvflg") == ['CM']
