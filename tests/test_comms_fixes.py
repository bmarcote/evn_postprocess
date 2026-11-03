"""comms: replies are matched to their run, and Mattermost's error body reaches the log."""
import io
import urllib.error
from evn_postprocess import comms


def test_reply_matching():
    assert comms._reply_is_for({"message": "anything"}, "p1", None)
    assert comms._reply_is_for({"message": "polswap: Wb", "root_id": "p1"}, "p1", "ER057B")
    assert not comms._reply_is_for({"message": "polswap: Wb", "root_id": "p2"}, "p1", "ER057B")
    assert comms._reply_is_for({"message": "er057b\npolswap: Wb"}, "p1", "ER057B")
    assert not comms._reply_is_for({"message": "polswap: Wb"}, "p1", "ER057B")


def test_http_error_keeps_the_body():
    exc = urllib.error.HTTPError("http://x/api/v4/posts", 400, "Bad Request", {},
                                 io.BytesIO(b'{"message": "MaxPostSize exceeded"}'))
    assert "MaxPostSize exceeded" in str(comms._with_error_body(exc))


def test_pi_address_is_quoted_when_needed():
    from evn_postprocess.distribution import piletter
    assert piletter._address('Smith, J.', 'a@b.org') == '"Smith, J." <a@b.org>'
    assert piletter._address('José Pérez', 'j@b.org') == 'José Pérez <j@b.org>'



def test_letter_files_get_the_default_permissions(tmp_path):
    """Same permissions as every other experiment file (no owner-only letters)."""
    import os
    from evn_postprocess.distribution import piletter
    path = tmp_path / "e.piletter_auth"
    piletter._write(path, "letter")
    umask = os.umask(0)
    os.umask(umask)
    assert os.stat(path).st_mode & 0o777 == 0o666 & ~umask
