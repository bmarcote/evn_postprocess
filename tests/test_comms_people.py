"""Tests for the people directory of comms.toml and the recipient resolution.

`postprocess` notifies the support scientist assigned to the experiment. On the shared
`jops` account the login name is not a person, so the assignment comes from the .jex file
and the address from the `[[people]]` table (username -> email / Mattermost username).
"""
from __future__ import annotations

import datetime as dt
from unittest.mock import Mock

from evn_postprocess import comms, experiment
from evn_postprocess import main as main_mod


PEOPLE_TOML = '''\
mode = "mattermost"
username = ""

[[people]]
username = "Marcote"
email = "marcote@jive.eu"
mattermost = "marcote"

[[people]]
username = "nair"
email = "nair@jive.eu"
mattermost = "dnair"

[mattermost]
server_url = "https://coms.example.org"
token = "t"
'''


def _make_exp(tmp_path, supsci):
    dirs = experiment.Dirs(logs=tmp_path / 'logs', plots=tmp_path / 'plots',
                           pipeline=tmp_path / 'p', pipe_in=tmp_path / 'p/in',
                           pipe_out=tmp_path / 'p/out', pipe_temp=tmp_path / 'p/tmp')
    exp = experiment.Experiment('EB101', dt.date(2026, 4, 10), supsci, dirs)
    exp.mode = experiment.Mode.supsci
    return exp


def _config(tmp_path, text=PEOPLE_TOML, name='comms.toml'):
    (tmp_path / name).write_text(text)
    return comms.CommsConfig.load(tmp_path / name)


class TestPeopleDirectory:
    def test_entries_are_parsed_and_keyed_case_insensitively(self, tmp_path):
        config = _config(tmp_path)
        assert set(config.people) == {'marcote', 'nair'}
        assert config.people['marcote'].email == 'marcote@jive.eu'
        assert config.people['nair'].mattermost == 'dnair'

    def test_an_entry_without_a_username_is_ignored(self, tmp_path):
        config = _config(tmp_path, 'mode = "email"\n\n[[people]]\nemail = "x@y.z"\n')
        assert config.people == {}

    def test_no_people_table_is_not_an_error(self, tmp_path):
        assert _config(tmp_path, 'mode = "none"\n').people == {}


class TestRecipientFor:
    def test_explicit_username_wins(self, tmp_path):
        config = _config(tmp_path, PEOPLE_TOML.replace('username = ""', 'username = "someone"', 1))
        assert comms.recipient_for(config, 'marcote') == 'someone'

    def test_mattermost_mode_uses_the_mattermost_username(self, tmp_path):
        assert comms.recipient_for(_config(tmp_path), 'Marcote') == 'marcote'
        assert comms.recipient_for(_config(tmp_path), 'nair') == 'dnair'

    def test_email_mode_uses_the_email_address(self, tmp_path):
        config = _config(tmp_path, PEOPLE_TOML.replace('mode = "mattermost"', 'mode = "email"'))
        assert comms.recipient_for(config, 'MARCOTE') == 'marcote@jive.eu'

    def test_unknown_support_scientist_resolves_to_nobody(self, tmp_path):
        assert comms.recipient_for(_config(tmp_path), 'nobody') == ''
        assert comms.recipient_for(_config(tmp_path), '') == ''

    def test_missing_address_for_the_mode_resolves_to_nobody(self, tmp_path):
        config = _config(tmp_path, PEOPLE_TOML.replace('mattermost = "marcote"', 'mattermost = ""'))
        assert comms.recipient_for(config, 'marcote') == ''

    def test_from_address_falls_back_to_the_recipient(self, tmp_path):
        # `from_address = ""` is present-but-empty; it must still default to the recipient.
        config = _config(tmp_path, 'mode = "email"\nusername = "a@b.c"\n\n'
                                   '[email]\nfrom_address = ""\n')
        assert config.smtp_from == 'a@b.c'


class TestSupportScientistFromJex:
    def test_reads_the_support_field(self, monkeypatch):
        from evn_postprocess.retrieval.jive import JiveRetriever
        monkeypatch.setattr('evn_postprocess.retrieval.jive.fetch_jexp_info',
                            lambda expname: {'support': 'marcote'})
        assert JiveRetriever().fetch_support_scientist(Mock(expname='EB101')) == 'marcote'

    def test_two_names_keep_the_later_one(self, monkeypatch):
        from evn_postprocess.retrieval.jive import JiveRetriever
        monkeypatch.setattr('evn_postprocess.retrieval.jive.fetch_jexp_info',
                            lambda expname: {'support': 'nair / marcote'})
        assert JiveRetriever().fetch_support_scientist(Mock(expname='EB101')) == 'marcote'

    def test_a_failed_lookup_returns_nobody(self, monkeypatch):
        from evn_postprocess.retrieval import RetrievalError
        from evn_postprocess.retrieval.jive import JiveRetriever

        def boom(expname):
            raise RetrievalError('no .jex on the server')

        monkeypatch.setattr('evn_postprocess.retrieval.jive.fetch_jexp_info', boom)
        assert JiveRetriever().fetch_support_scientist(Mock(expname='EB101')) == ''

    def test_the_none_backend_knows_nobody(self):
        from evn_postprocess.retrieval.local import NoneRetriever
        assert NoneRetriever().fetch_support_scientist(Mock(expname='EB101')) == ''


class TestResolveSupportScientist:
    """`exp.supsci` must name a person: it picks the AIPS user number, signs the pipeline
    feedback page and addresses the notifications. Under the shared 'jops' login it is
    read from the experiment's .jex file.
    """

    def _resolve(self, tmp_path, monkeypatch, supsci, cli_supsci=None, jex_support='marcote'):
        monkeypatch.setattr('evn_postprocess.retrieval.jive.fetch_jexp_info',
                            lambda expname: {'support': jex_support})
        exp = _make_exp(tmp_path, supsci)
        main_mod._resolve_support_scientist(exp, cli_supsci)
        return exp.supsci

    def test_a_named_support_scientist_is_left_alone(self, tmp_path, monkeypatch):
        assert self._resolve(tmp_path, monkeypatch, 'nair') == 'nair'

    def test_the_shared_account_is_replaced_from_the_jex(self, tmp_path, monkeypatch):
        assert self._resolve(tmp_path, monkeypatch, 'jops') == 'marcote'

    def test_jss_wins_over_the_jex(self, tmp_path, monkeypatch):
        assert self._resolve(tmp_path, monkeypatch, 'jops', cli_supsci='nair') == 'nair'

    def test_an_unknown_assignment_keeps_the_login(self, tmp_path, monkeypatch):
        assert self._resolve(tmp_path, monkeypatch, 'jops', jex_support='') == 'jops'


class TestConfigureComms:
    def _notifier(self, tmp_path, monkeypatch, supsci):
        (tmp_path / 'comms.toml').write_text(PEOPLE_TOML)
        monkeypatch.chdir(tmp_path)
        sent: list = []
        monkeypatch.setattr(main_mod.workflow, 'set_notifier', sent.append)
        main_mod._configure_comms(_make_exp(tmp_path, supsci), Mock(comms=None))
        return sent[0].config.username if sent else None

    def test_the_support_scientist_of_the_experiment_is_notified(self, tmp_path, monkeypatch):
        assert self._notifier(tmp_path, monkeypatch, 'nair') == 'dnair'
        assert self._notifier(tmp_path, monkeypatch, 'Marcote') == 'marcote'

    def test_nobody_to_notify_leaves_the_notifier_unset(self, tmp_path, monkeypatch):
        assert self._notifier(tmp_path, monkeypatch, 'someone-else') is None

    def test_mode_none_never_sets_a_notifier(self, tmp_path, monkeypatch):
        (tmp_path / 'comms.toml').write_text('mode = "none"\n')
        monkeypatch.chdir(tmp_path)
        sent: list = []
        monkeypatch.setattr(main_mod.workflow, 'set_notifier', sent.append)
        main_mod._configure_comms(_make_exp(tmp_path, 'marcote'), Mock(comms=None))
        assert sent == []
