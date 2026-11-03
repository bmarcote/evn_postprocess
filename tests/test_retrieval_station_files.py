"""Station files are recognised as {exp}{ant}.ext (and {exp}_{ant}...), for every retrieval backend."""
from pathlib import Path
from evn_postprocess.retrieval import station_files as retrieval


def test_station_of_file():
    assert retrieval.station_of_file(Path("er057bef.antabfs"), "ER057B") == "Ef"
    assert retrieval.station_of_file(Path("er057b_o8_1.antabfs"), "ER057B") == "O8"
    assert retrieval.station_of_file(Path("er057bcm.log"), "er057b") == "Cm"


class _Ant:
    def __init__(self):
        self.logfsfile = self.antabfsfile = self.opacity = False


class _Antennas(dict):
    @property
    def observed(self):
        return list(self)


class _Exp:
    expname = "ER057B"

    def __init__(self, tmp_path):
        from types import SimpleNamespace
        self.dirs = SimpleNamespace(pipe_temp=tmp_path)
        self.antennas = _Antennas(Ef=_Ant(), Cm=_Ant())

    def store(self):
        pass


def test_register_flags_and_opacity(tmp_path):
    (tmp_path / "er057bef.antabfs").write_text("POLY=1.0,opacity_corrected /\n")
    (tmp_path / "er057bef.log").write_text("x")
    (tmp_path / "er057bxx.log").write_text("x")  # another experiment's antenna: ignored
    exp = _Exp(tmp_path)
    retrieval.register_station_files(exp)
    assert exp.antennas['Ef'].antabfsfile and exp.antennas['Ef'].logfsfile and exp.antennas['Ef'].opacity
    assert not exp.antennas['Cm'].logfsfile
    assert "! opacity_corrected" in (tmp_path / "er057bef.antabfs").read_text()


def test_none_retriever_finds_exp_prefixed_files(tmp_path):
    from evn_postprocess.retrieval import local
    (tmp_path / "er057bef.antabfs").write_text("x")
    (tmp_path / "er057bef.log").write_text("x")
    exp = _Exp(tmp_path)
    assert local.NoneRetriever().fetch_station_files(exp) is True
    assert exp.antennas['Ef'].antabfsfile and exp.antennas['Ef'].logfsfile
