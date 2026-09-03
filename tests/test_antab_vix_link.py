"""Tests for pipeline._link_vixfile: the vex schedule sits beside the station files.

antab_editor.py runs from ``antenna_files/``, so the .vix is linked in there before the
editor opens. The link is named after the lowercase experiment (what the editor looks
for), it is relative (the experiment directory gets moved around), and the helper must
never be what breaks the antab step.
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import Mock

import pytest

from evn_postprocess import experiment
from evn_postprocess import pipeline


@pytest.fixture
def exp(tmp_path, monkeypatch):
    """A minimal experiment rooted at tmp_path, with antenna_files/ and a real .vix."""
    monkeypatch.chdir(tmp_path)
    pipe_temp = Path('antenna_files')
    pipe_temp.mkdir()
    stub = Mock(spec=experiment.Experiment)
    stub.expname = 'ES124'
    stub.dirs = Mock()
    stub.dirs.pipe_temp = pipe_temp
    stub.vixfile = Path('ES124.vix')
    stub.vixfile.write_text('VEX_rev = 1.5;\n')
    return stub


class TestLinkVixfile:
    def test_links_the_vex_into_antenna_files(self, exp):
        """The link carries the lowercase experiment name: antab_editor.py looks for that one,
        not for the uppercase `exp.vixfile` of the experiment root."""
        pipeline._link_vixfile(exp)
        link = exp.dirs.pipe_temp / 'es124.vix'
        assert link.is_symlink()
        assert link.read_text() == 'VEX_rev = 1.5;\n'
        assert not (exp.dirs.pipe_temp / 'ES124.vix').exists()

    def test_the_link_is_relative(self, exp, tmp_path):
        # An absolute link would break the moment the experiment directory is moved.
        pipeline._link_vixfile(exp)
        target = os.readlink(exp.dirs.pipe_temp / 'es124.vix')
        assert not os.path.isabs(target), target
        assert target == os.path.join('..', 'ES124.vix')   # the file in the root keeps its name

    def test_survives_the_experiment_directory_being_moved(self, exp, tmp_path):
        pipeline._link_vixfile(exp)
        moved = tmp_path.parent / f'{tmp_path.name}-moved'
        tmp_path.rename(moved)
        try:
            assert (moved / 'antenna_files' / 'es124.vix').read_text() == 'VEX_rev = 1.5;\n'
        finally:
            moved.rename(tmp_path)

    def test_running_twice_is_a_no_op(self, exp):
        pipeline._link_vixfile(exp)
        pipeline._link_vixfile(exp)  # must not raise FileExistsError
        assert (exp.dirs.pipe_temp / 'es124.vix').is_symlink()

    def test_a_dangling_link_is_replaced(self, exp):
        link = exp.dirs.pipe_temp / 'es124.vix'
        link.symlink_to('../gone.vix')
        assert not link.exists()  # dangling
        pipeline._link_vixfile(exp)
        assert link.read_text() == 'VEX_rev = 1.5;\n'

    def test_a_real_file_already_there_is_left_alone(self, exp):
        link = exp.dirs.pipe_temp / 'es124.vix'
        link.write_text('someone put a real vex here\n')
        pipeline._link_vixfile(exp)
        assert not link.is_symlink()
        assert link.read_text() == 'someone put a real vex here\n'

    def test_a_missing_vex_is_only_a_warning(self, exp):
        exp.vixfile.unlink()
        pipeline._link_vixfile(exp)  # must not raise
        assert not (exp.dirs.pipe_temp / 'es124.vix').exists()


class TestAntabEditorExitCode:
    """antab_editor.py exits 127 when its window is closed: a normal end of session here,
    not the shell's 'command not found', and it must not fail the step."""

    def test_127_is_accepted(self, exp, monkeypatch, tmp_path):
        seen = {}

        def fake(command, parameters=None, **kwargs):
            seen['ok'] = kwargs.get('ok_returncodes')
            return ''

        monkeypatch.setattr(pipeline.utils, 'shell_command', fake)
        monkeypatch.setattr(pipeline.lisfiles, '_pass_lisfiles', lambda _p: [])
        exp.eEVNname = None
        exp.antennas = []
        assert pipeline.run_antab_editor(exp) is True
        assert seen['ok'] == (127,)
        assert pipeline.ANTAB_EDITOR_OK_RETURNCODES == (127,)

    def test_any_other_code_still_fails(self, exp, monkeypatch):
        """Only 127 is excused: a real crash must still surface."""
        def boom(command, parameters=None, **kwargs):
            raise ValueError(f"'{command}' exited with code 1 (see output above).")

        monkeypatch.setattr(pipeline.utils, 'shell_command', boom)
        monkeypatch.setattr(pipeline.lisfiles, '_pass_lisfiles', lambda _p: [])
        exp.eEVNname = None
        exp.antennas = []
        with pytest.raises(ValueError, match="exited with code 1"):
            pipeline.run_antab_editor(exp)
