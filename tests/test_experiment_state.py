"""Tests for evn_postprocess.experiment_state (experiment TOML round-trip).

Covers: loading/validation, scan-selection parsing, write-back round-trips with
byte-identical user sections, and the toml-over-policy precedence rule.
Follows the fixture style of test_policy.py: literal toml strings in tmp_path.
"""
import pytest

from evn_postprocess import experiment_state as es
from evn_postprocess.policy import Policy


FULL_TOML = '''\
# User comment at the top: must survive any write-back.
[observation]
expname = "EB101"  # inline comment, must survive too
supsci = "marcote"
scans = "1-5,20-22"

[[pi]]
name = "Jane Doe"
email = "jane.doe@institute.edu"

[[pi]]
name = "John Smith"
email = "john.smith@other.edu"

[sources."J1848+3244"]
type = "target"
protected = true

[sources."3C345"]
type = "fringefinder"

[retrieval]
mode = "none"

[pipeline]
mode = "aips"

[distribution]
mode = "none"

[postprocess]
weight_threshold = 0.9
polswap = ["Wb"]
onebit = []
refant = ["Ef", "Mc"]

[postprocess.gain_corrections]
Ef = 1.0
Wb = 1.12

[comments]
general = "All good."

[comments.stations.Ef]
status = "success"
note = ""

[comments.stations.Wb]
status = "minor"
note = "Missed 09:30-10:15 UT."
'''


def write_toml(tmp_path, content, name="eb101.toml"):
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


# --------------------------------------------------------------------- loading

def test_load_full_toml(tmp_path):
    t = es.load_toml(write_toml(tmp_path, FULL_TOML))
    assert t.observation.expname == "EB101"
    assert t.observation.supsci == "marcote"
    assert t.observation.scans == [1, 2, 3, 4, 5, 20, 21, 22]
    assert [pi.name for pi in t.pis] == ["Jane Doe", "John Smith"]
    assert t.pis[0].email == "jane.doe@institute.edu"
    assert t.sources["J1848+3244"].type == "target"
    assert t.sources["J1848+3244"].protected is True
    assert t.sources["J1848+3244"].guessed is False
    assert t.sources["3C345"].type == "fringefinder"
    assert t.retrieval == "none"
    assert t.pipeline == "aips"
    assert t.distribution == "none"
    assert t.postprocess.weight_threshold == 0.9
    assert t.postprocess.polswap == ["Wb"]
    assert t.postprocess.onebit == []          # explicit empty list, not None
    assert t.postprocess.polconvert is None    # absent, not empty
    assert t.postprocess.refant == ["Ef", "Mc"]
    assert t.postprocess.gain_corrections == {"Ef": 1.0, "Wb": 1.12}
    assert t.comments.general == "All good."
    assert t.comments.stations["Wb"].status == "minor"
    assert t.comments.stations["Wb"].note == "Missed 09:30-10:15 UT."


def test_load_partial_toml(tmp_path):
    t = es.load_toml(write_toml(tmp_path, '[observation]\nexpname = "EB101"\n'))
    assert t.observation.expname == "EB101"
    assert t.observation.scans is None
    assert t.pis == []
    assert t.sources == {}
    assert t.retrieval is None
    assert t.postprocess.weight_threshold is None
    assert t.comments.general == ""


def test_load_empty_file(tmp_path):
    t = es.load_toml(write_toml(tmp_path, ""))
    assert t.observation.expname is None
    assert t.pis == []


def test_load_missing_file_returns_empty_container(tmp_path):
    path = tmp_path / "eb101.toml"
    t = es.load_toml(path)  # must not raise
    assert t.path == path
    assert t.observation.expname is None
    assert not path.exists()


def test_load_malformed_toml_names_file(tmp_path):
    path = write_toml(tmp_path, "[observation\nexpname=")
    with pytest.raises(es.ExperimentTomlError) as excinfo:
        es.load_toml(path)
    assert path.name in str(excinfo.value)


def test_load_wrong_type_names_section_and_key(tmp_path):
    path = write_toml(tmp_path, '[observation]\nexpname = 42\n')
    with pytest.raises(es.ExperimentTomlError) as excinfo:
        es.load_toml(path)
    assert "observation" in str(excinfo.value) and "expname" in str(excinfo.value)


def test_load_unknown_keys_ignored(tmp_path):
    t = es.load_toml(write_toml(tmp_path, '[observation]\nfuture_key = "x"\nexpname = "EB101"\n'))
    assert t.observation.expname == "EB101"


def test_load_invalid_source_type(tmp_path):
    path = write_toml(tmp_path, '[sources."X"]\ntype = "quasar"\n')
    with pytest.raises(es.ExperimentTomlError) as excinfo:
        es.load_toml(path)
    assert "quasar" in str(excinfo.value)


def test_load_invalid_station_status(tmp_path):
    path = write_toml(tmp_path, '[comments.stations.Ef]\nstatus = "green"\n')
    with pytest.raises(es.ExperimentTomlError) as excinfo:
        es.load_toml(path)
    assert "green" in str(excinfo.value) and "Ef" in str(excinfo.value)


# ---------------------------------------------------------------- scan parsing

@pytest.mark.parametrize("value,expected", [
    ("4", [4]),
    (4, [4]),
    ("3-10", [3, 4, 5, 6, 7, 8, 9, 10]),
    ("1-5,20-30", list(range(1, 6)) + list(range(20, 31))),
    ("1-3, 7 ,9-10", [1, 2, 3, 7, 9, 10]),
    (["1-3", 7, "9-10"], [1, 2, 3, 7, 9, 10]),
    (["1-5", "3-8"], list(range(1, 9))),   # overlap: deduplicated, ordered
    (None, None),
])
def test_parse_scan_selection_valid(value, expected):
    assert es.parse_scan_selection(value) == expected


@pytest.mark.parametrize("value,fragment", [
    ("10-3", "10-3"),        # reversed range
    ("abc", "abc"),          # garbage token
    ("-5", "-5"),            # negative / malformed
    ("1-2-3", "1-2-3"),      # too many dashes
    (0, "0"),                # non-positive
    (True, "bool"),          # booleans are not scan numbers
])
def test_parse_scan_selection_invalid(value, fragment):
    with pytest.raises(es.ExperimentTomlError) as excinfo:
        es.parse_scan_selection(value)
    assert fragment in str(excinfo.value)


# ------------------------------------------------------------------ write-back

def user_section_text(content: str) -> str:
    """The user-owned part of FULL_TOML (everything before [postprocess])."""
    return content.split("[postprocess]")[0]


def test_record_parameters_roundtrip_preserves_user_sections(tmp_path):
    path = write_toml(tmp_path, FULL_TOML)
    t = es.load_toml(path)
    t.record_parameters(weight_threshold=0.85, polconvert=["Kt"], flagged_percent=3.2,
                        antab_files=["eb101.antab"], gain_corrections={"Ef": 1.05})
    t.save()
    saved = path.read_text(encoding="utf-8")
    # User sections (incl. comments and formatting) byte-identical:
    assert saved.startswith(user_section_text(FULL_TOML))
    reloaded = es.load_toml(path)
    assert reloaded.postprocess.weight_threshold == 0.85
    assert reloaded.postprocess.polconvert == ["Kt"]
    assert reloaded.postprocess.flagged_percent == 3.2
    assert reloaded.postprocess.antab_files == ["eb101.antab"]
    assert reloaded.postprocess.gain_corrections["Ef"] == 1.05
    # Pre-existing [postprocess] values not passed in the call are preserved:
    assert reloaded.postprocess.polswap == ["Wb"]
    assert reloaded.postprocess.refant == ["Ef", "Mc"]


def test_record_parameters_unknown_key(tmp_path):
    t = es.load_toml(tmp_path / "eb101.toml")
    with pytest.raises(es.ExperimentTomlError) as excinfo:
        t.record_parameters(weight_treshold=0.9)  # typo on purpose
    assert "weight_treshold" in str(excinfo.value)


def test_record_comments_updates_without_duplicating(tmp_path):
    path = write_toml(tmp_path, FULL_TOML)
    t = es.load_toml(path)
    t.record_comments(general="First.", stations={"Tr": es.StationComment("major", "Did not observe.")})
    t.record_comments(general="Second.", stations={"Tr": es.StationComment("minor", "Partly recovered.")})
    t.save()
    saved = path.read_text(encoding="utf-8")
    assert saved.count("[comments.stations.Tr]") == 1
    reloaded = es.load_toml(path)
    assert reloaded.comments.general == "Second."
    assert reloaded.comments.stations["Tr"].status == "minor"
    assert reloaded.comments.stations["Wb"].status == "minor"  # untouched entry preserved


def test_record_comments_invalid_status(tmp_path):
    t = es.load_toml(tmp_path / "eb101.toml")
    with pytest.raises(es.ExperimentTomlError):
        t.record_comments(stations={"Ef": es.StationComment("red", "")})


def test_record_sources_never_overrides_user_entries(tmp_path):
    path = write_toml(tmp_path, FULL_TOML)
    t = es.load_toml(path)
    t.record_sources({"J1848+3244": "other", "J0000+0000": "calibrator"})
    t.save()
    reloaded = es.load_toml(path)
    assert reloaded.sources["J1848+3244"].type == "target"       # user-set: untouched
    assert reloaded.sources["J1848+3244"].guessed is False
    assert reloaded.sources["J0000+0000"].type == "calibrator"   # new guess recorded
    assert reloaded.sources["J0000+0000"].guessed is True
    # A later, better guess may update a previous guess:
    t2 = es.load_toml(path)
    t2.record_sources({"J0000+0000": "target"})
    t2.save()
    assert es.load_toml(path).sources["J0000+0000"].type == "target"


def test_writeback_creates_file_when_absent(tmp_path):
    path = tmp_path / "eb101.toml"
    t = es.load_toml(path)
    t.record_parameters(weight_threshold=0.7)
    t.save()
    assert path.exists()
    assert es.load_toml(path).postprocess.weight_threshold == 0.7


def test_stray_tempfile_does_not_corrupt_save(tmp_path):
    path = write_toml(tmp_path, FULL_TOML)
    (tmp_path / ".eb101.toml.stray.tmp").write_text("garbage", encoding="utf-8")
    t = es.load_toml(path)
    t.record_parameters(weight_threshold=0.8)
    t.save()
    reloaded = es.load_toml(path)
    assert reloaded.postprocess.weight_threshold == 0.8
    assert reloaded.observation.expname == "EB101"


# ------------------------------------------------------------------ precedence

def test_precedence_toml_wins_over_policy(tmp_path):
    t = es.load_toml(write_toml(tmp_path, FULL_TOML))
    policy = Policy(weight_threshold=0.5, polswap=["Mc"], refant=["Jb"])
    r = es.resolve_parameters(t, policy)
    assert r.weight_threshold == 0.9      # toml wins
    assert r.polswap == ["Wb"]            # toml wins
    assert r.refant == ["Ef", "Mc"]       # toml wins
    assert r.onebit == []                 # explicit empty list in toml means "none", wins
    assert r.retrieval == "none" and r.pipeline == "aips" and r.distribution == "none"


def test_precedence_policy_fills_gaps(tmp_path):
    t = es.load_toml(write_toml(tmp_path, '[observation]\nexpname = "EB101"\n'))
    policy = Policy(weight_threshold=0.5, polconvert=["Kt"])
    r = es.resolve_parameters(t, policy)
    assert r.weight_threshold == 0.5
    assert r.polconvert == ["Kt"]
    assert r.polswap is None and "polswap" in r.unresolved  # policy [] means unset
    assert r.retrieval == "jive" and r.pipeline == "aips" and r.distribution == "jive"


def test_precedence_nothing_defined(tmp_path):
    r = es.resolve_parameters(es.load_toml(tmp_path / "eb101.toml"), None)
    assert r.weight_threshold is None
    assert set(r.unresolved) == {"weight_threshold", "polswap", "polconvert", "onebit", "refant"}


def test_precedence_empty_list_vs_absent(tmp_path):
    # onebit = [] in the toml is an explicit "no antenna", NOT unset:
    t = es.load_toml(write_toml(tmp_path, '[postprocess]\nonebit = []\n'))
    r = es.resolve_parameters(t, Policy(onebit=["Sh"]))
    assert r.onebit == []
    assert "onebit" not in r.unresolved


def test_precedence_scans_from_toml(tmp_path):
    t = es.load_toml(write_toml(tmp_path, '[observation]\nscans = "3-4"\n'))
    assert es.resolve_parameters(t, None).scans == [3, 4]


def test_record_pi_appends_without_duplicating(tmp_path):
    path = write_toml(tmp_path, FULL_TOML)
    t = es.load_toml(path)
    t.record_pi([{'name': 'Jane Doe', 'email': 'jane.doe@institute.edu'},   # already there
                 {'name': 'New Contact', 'email': 'new@x.edu'}])
    t.save()
    reloaded = es.load_toml(path)
    assert [pi.email for pi in reloaded.pis] == ['jane.doe@institute.edu',
                                                 'john.smith@other.edu', 'new@x.edu']
    # And in a toml without any [[pi]] section:
    t2 = es.load_toml(tmp_path / 'other.toml')
    t2.record_pi([{'name': 'A', 'email': 'a@b.c'}])
    t2.save()
    assert es.load_toml(tmp_path / 'other.toml').pis[0].email == 'a@b.c'


# ----------------------------------------------------------------- end to end

def test_example_file_matches_schema():
    """The committed docs/experiment.toml.example must always parse against the schema."""
    from pathlib import Path
    example = Path(__file__).parent.parent / "docs" / "experiment.toml.example"
    t = es.load_toml(example)
    assert t.observation.expname == "EB101"
    assert t.pis and t.pis[0].name == "Jane Doe"
    assert t.sources["J1848+3244"].type == "target"
    assert t.postprocess.weight_threshold == 0.9
    assert t.comments.stations["Tr"].status == "major"


def test_end_to_end_toml_only_directory(tmp_path):
    """A directory with only EXPNAME.toml: load -> resolve -> record -> reload cycle."""
    import shutil
    from pathlib import Path
    example = Path(__file__).parent.parent / "docs" / "experiment.toml.example"
    path = tmp_path / "eb101.toml"
    shutil.copy(example, path)
    t = es.load_toml(path)
    r = es.resolve_parameters(t, None)
    assert r.weight_threshold == 0.9 and r.unresolved == []
    assert r.retrieval == "jive" and r.pipeline == "aips" and r.distribution == "jive"
    t.record_parameters(weight_threshold=0.95)
    t.record_comments(general="Reviewed.")
    t.save()
    reloaded = es.load_toml(path)
    assert reloaded.postprocess.weight_threshold == 0.95
    assert reloaded.comments.general == "Reviewed."
    assert reloaded.comments.stations["Wb"].status == "minor"  # example content preserved
    assert reloaded.pis[1].name == "John Smith"


# ---------------------------------------------------------------- info summary

def test_summary_lines_mark_origin(tmp_path):
    path = write_toml(tmp_path, FULL_TOML)
    lines = es.summary_lines(es.load_toml(path))
    text = "\n".join(lines)
    assert "EB101" in text and "Jane Doe" in text and "J1848+3244" in text
    assert all(path.name in line for line in lines)  # every value states its origin
