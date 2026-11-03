"""Dashboard edits (source type, refant) must reach the passes and must not overwrite newer stored state."""
from types import SimpleNamespace
from evn_postprocess import experiment, plotting


class _Sources(dict):
    @property
    def names(self):
        return list(self)


def _src(t):
    return SimpleNamespace(type=t)


def test_set_source_type_reaches_every_pass():
    exp = SimpleNamespace(sources=_Sources(J1905=_src('other')),
                          correlator_passes=[SimpleNamespace(sources=_Sources(J1905=_src('other'))),
                                             SimpleNamespace(sources=None)])
    experiment.set_source_type(exp, ['J1905'], 'calibrator')
    assert exp.sources['J1905'].type == 'calibrator'
    assert exp.correlator_passes[0].sources['J1905'].type == 'calibrator'


class _Exp:
    """Stands in for an Experiment: records stores, carries one 'progress' field."""
    def __init__(self, progress):
        self.refant, self.progress, self.stored = ['Ef'], progress, False
        self._local_copy = 'x.json'

    def store(self, path=None):
        self.stored = True


def test_edit_is_merged_into_the_stored_json_not_the_stale_copy(monkeypatch):
    served = _Exp(progress='old')            # the dashboard loaded this at start-up
    on_disk = _Exp(progress='newer')         # a concurrent run stored more progress since
    monkeypatch.setattr(experiment.Experiment, 'load', staticmethod(lambda expname=None, path=None: on_disk))
    monkeypatch.setattr(plotting._DashboardHandler, 'exp', served)
    handler = plotting._DashboardHandler.__new__(plotting._DashboardHandler)
    handler._apply_and_store(lambda e: setattr(e, 'refant', ['O8', 'Ef']))
    assert on_disk.stored and on_disk.progress == 'newer' and on_disk.refant == ['O8', 'Ef']
    assert not served.stored and served.refant == ['O8', 'Ef']


def test_falls_back_to_storing_the_served_copy_when_json_unreadable(monkeypatch):
    served = _Exp(progress='old')

    def broken(expname=None, path=None):
        raise FileNotFoundError('gone')
    monkeypatch.setattr(experiment.Experiment, 'load', staticmethod(broken))
    monkeypatch.setattr(plotting._DashboardHandler, 'exp', served)
    handler = plotting._DashboardHandler.__new__(plotting._DashboardHandler)
    handler._apply_and_store(lambda e: setattr(e, 'refant', ['O8']))
    assert served.stored and served.refant == ['O8']
