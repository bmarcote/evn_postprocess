"""Tests for the resume-time output checks (_validate_outputs) of the steps that already ran."""
import os
import time
from pathlib import Path
from types import SimpleNamespace
from evn_postprocess import workflow


def make_exp(tmp_path: Path):
    ms = tmp_path / "exp.ms"
    ms.mkdir()
    dirs = SimpleNamespace(pipe_in=tmp_path / "pipeline" / "in", pipe_out=tmp_path / "pipeline" / "out")
    dirs.pipe_in.mkdir(parents=True)
    dirs.pipe_out.mkdir(parents=True)
    passes = [SimpleNamespace(msfile=ms, lisfile=tmp_path / "exp.lis", fitsidifile="exp_1_1.IDI")]
    return SimpleNamespace(expname="EXP", correlator_passes=passes, dirs=dirs,
                           antennas=SimpleNamespace(polconvert=[]))


def steps(*names):
    return [workflow.Task(name=n, command=n, doc='', done=True) for n in names]


def age(path: Path, seconds_ago: float):
    t = time.time() - seconds_ago
    os.utime(path, (t, t))


def test_postpipe_tasav_does_not_delete_pipeline_outputs(tmp_path, monkeypatch):
    """postpipe writes {exp}.tasav.txt into pipeline/in after the pipeline ran: outputs must stay."""
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    for name, ago in (("exp.inp.txt", 300), ("exp.antab", 300), ("exp.uvflg", 300)):
        (exp.dirs.pipe_in / name).write_text("x")
        age(exp.dirs.pipe_in / name, ago)
    (exp.dirs.pipe_out / "exp.SCAN").write_text("x")
    age(exp.dirs.pipe_out / "exp.SCAN", 200)
    (exp.dirs.pipe_in / "exp.tasav.txt").write_text("x")  # newest: written by postpipe
    all_steps = steps('pipeline', 'postpipe')
    workflow._validate_outputs(exp, all_steps)
    assert (exp.dirs.pipe_out / "exp.SCAN").exists()
    assert all(s.done for s in all_steps)


def test_pipeline_outputs_older_than_a_real_input_are_reset(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    (exp.dirs.pipe_out / "exp.SCAN").write_text("x")
    age(exp.dirs.pipe_out / "exp.SCAN", 200)
    (exp.dirs.pipe_in / "exp.inp.txt").write_text("x")  # edited after the pipeline ran
    all_steps = steps('pipeline', 'postpipe')
    workflow._validate_outputs(exp, all_steps)
    assert not any(s.done for s in all_steps)


def test_gzipped_standard_plots_count_after_delivery(tmp_path, monkeypatch):
    """distribute gzips every *.ps: a delivered experiment must not re-run from standardplots."""
    monkeypatch.chdir(tmp_path)
    exp = make_exp(tmp_path)
    age(exp.correlator_passes[0].msfile, 300)
    (tmp_path / "exp-cross-scan1.ps.gz").write_text("x")
    all_steps = steps('standardplots', 'distribute')
    workflow._validate_outputs(exp, all_steps)
    assert all(s.done for s in all_steps)
