"""Server-access boundary invariant (Phase 2, Issue 4, story 17).

Every outbound ssh/scp/rsync for the package must live inside the JIVE backend modules.
After the relocation this means only two files may call the ``utils`` remote helpers:

  - ``retrieval/jive.py`` -- the JIVE retrieval backend (ccs/vlbeer/piletters transport);
  - ``process.py``        -- the sanctioned, temporary ``--tConvert-in-eee`` workaround.

This is a pure text scan of the source tree, so it is casacore-free and runs anywhere.
It guards against a future edit re-introducing server access into a core module.
"""
from __future__ import annotations

import re
from pathlib import Path

SRC = Path(__file__).parent.parent / "src" / "evn_postprocess"

# The remote-transport helpers defined in utils. A *call* to any of these is server access.
REMOTE_CALL = re.compile(r"utils\.(?:ssh|scp|rsync|remote_file_exists)\(")
# A raw ssh invocation via subprocess (belt-and-braces: catches bypassing the helpers).
RAW_SSH = re.compile(r"""(?:getoutput|run|Popen|call|check_output)\(\s*['"\[][^)]*\bssh\b""")

# Files allowed to perform outbound server access.
SANCTIONED = {"retrieval/jive.py", "process.py"}


def _offending_files() -> dict[str, list[str]]:
    offenders: dict[str, list[str]] = {}
    for py in SRC.rglob("*.py"):
        rel = py.relative_to(SRC).as_posix()
        if rel.startswith("scripts/"):
            continue  # standalone utilities, deliberately out of scope (story 18)
        if rel in SANCTIONED:
            continue
        text = py.read_text(encoding="utf-8")
        hits = REMOTE_CALL.findall(text) + RAW_SSH.findall(text)
        if hits:
            offenders[rel] = hits
    return offenders


def test_no_server_access_outside_jive_backends():
    offenders = _offending_files()
    assert not offenders, (
        "Outbound ssh/scp found outside the sanctioned JIVE backend modules "
        f"({', '.join(sorted(SANCTIONED))}): {offenders}. "
        "Server access for input/output belongs in retrieval/jive.py or distribution/jive.py."
    )


def test_positive_control_jive_backend_does_use_remote_helpers():
    # Guards against the scan passing vacuously (e.g. if the pattern silently broke).
    jive = (SRC / "retrieval" / "jive.py").read_text(encoding="utf-8")
    assert REMOTE_CALL.search(jive), "expected the JIVE retrieval backend to use utils remote helpers"
