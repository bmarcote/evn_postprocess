"""experiment toml: a single [pi] table can be extended, and a save keeps the file mode."""
import os
from evn_postprocess import experiment_state


def test_record_pi_on_a_single_pi_table(tmp_path):
    path = tmp_path / "e.toml"
    path.write_text('[pi]\nname = "A"\nemail = "a@x.org"\n')
    t = experiment_state.load_toml(path)
    t.record_pi([{'name': 'B', 'email': 'b@x.org'}])
    t.save()
    emails = [pi.email for pi in experiment_state.load_toml(path).pis]
    assert emails == ['a@x.org', 'b@x.org']


def test_save_keeps_the_existing_mode(tmp_path):
    path = tmp_path / "e.toml"
    path.write_text('[postprocess]\n')
    os.chmod(path, 0o644)
    experiment_state.load_toml(path).save()
    assert oct(os.stat(path).st_mode & 0o777) == oct(0o644)


def test_servers_default_config_dir_is_dot_config(tmp_path, monkeypatch):
    from evn_postprocess import servers
    monkeypatch.delenv('XDG_CONFIG_HOME', raising=False)
    monkeypatch.setenv('HOME', str(tmp_path))
    (tmp_path / '.config' / 'evn').mkdir(parents=True)
    (tmp_path / '.config' / 'evn' / 'computers.toml').write_text('[ccs]\nuser="u"\nhost="h"\npath="/p"\n')
    assert [s.name for s in servers.retrieve_servers()] == ['ccs']


def test_extra_phase_centres_are_targets():
    from types import SimpleNamespace
    from evn_postprocess import experiment, source_classify
    srcs = [SimpleNamespace(name=n, type=experiment.SourceType.other) for n in ('PC', 'T2')]
    exp = SimpleNamespace(scans=[SimpleNamespace(source='PC', phase_centers=('PC', 'T2'), duration_s=120,
                                            starttime=None)],
                          sources=srcs, exp_toml=None, expname='EG123A')
    guesses = source_classify.classify_sources(exp)
    assert guesses.get('T2') == 'target'
