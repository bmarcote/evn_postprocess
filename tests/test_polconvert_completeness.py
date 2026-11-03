"""PolConvert outputs count only with their .ok marker, so partial or failed conversions are never delivered."""
from pathlib import Path
from types import SimpleNamespace
from evn_postprocess import process


def touch(*names):
    for name in names:
        Path(name).write_text(name)


def test_unconverted_requires_marker(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    touch("e_1_1.IDI1.PCONVERT", "e_1_1.IDI1.PCONVERT.ok", "e_1_1.IDI2.PCONVERT")
    assert process._unconverted_idi_files(["e_1_1.IDI1", "e_1_1.IDI2", "e_1_1.IDI3"]) == ["e_1_1.IDI2", "e_1_1.IDI3"]


class _Antennas:
    polconvert = ['Ef']

    def __getitem__(self, name):
        return SimpleNamespace(subbands=[])


def test_polconvert_does_not_accept_a_partial_set(tmp_path, monkeypatch):
    """IDI2 has no converted output: the step must not report 'already done'."""
    monkeypatch.chdir(tmp_path)
    touch("e_1_1.IDI1", "e_1_1.IDI2", "e_1_1.IDI1.PCONVERT", "e_1_1.IDI1.PCONVERT.ok")
    exp = SimpleNamespace(expname="E", antennas=_Antennas())
    # With no recorded subbands it stops right after the skip check, returning False (not the skip's True).
    assert process.polconvert(exp) is False


def test_polconvert_skips_when_every_file_is_complete(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    touch("e_1_1.IDI1", "e_1_1.IDI1.PCONVERT", "e_1_1.IDI1.PCONVERT.ok")
    exp = SimpleNamespace(expname="E", antennas=SimpleNamespace(polconvert=['Ef']))
    assert process.polconvert(exp) is True


def test_post_post_refuses_incomplete_outputs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    touch("e_1_1.IDI1", "e_1_1.IDI1.PCONVERT")
    exp = SimpleNamespace(antennas=SimpleNamespace(polconvert=['Ef']))
    assert process.post_post_polconvert(exp) is False
    assert Path("e_1_1.IDI1").read_text() == "e_1_1.IDI1"  # untouched


def test_post_post_renames_drops_markers_and_keeps_existing_backup(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    touch("e_1_1.IDI1", "e_1_1.IDI1.PCONVERT", "e_1_1.IDI1.PCONVERT.ok")
    (tmp_path / "idi_ori").mkdir()
    (tmp_path / "idi_ori" / "e_1_1.IDI1").write_text("true original")
    exp = SimpleNamespace(antennas=SimpleNamespace(polconvert=['Ef']))
    assert process.post_post_polconvert(exp) is True
    assert Path("e_1_1.IDI1").read_text() == "e_1_1.IDI1.PCONVERT"
    assert not Path("e_1_1.IDI1.PCONVERT.ok").exists()
    assert (tmp_path / "idi_ori" / "e_1_1.IDI1").read_text() == "true original"
