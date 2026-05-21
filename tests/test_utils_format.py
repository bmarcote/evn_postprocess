"""Tests for evn_postprocess.utils.format_remote_path.

These cover the safer replacement for the previous ``eval(f"f'…'")`` call that
formatted server paths. The new helper only understands the two placeholder
shapes actually used by the project's TOML files.
"""
from __future__ import annotations

import datetime as dt

from evn_postprocess.utils import format_remote_path


class TestFormatRemotePath:
    def test_substitutes_expname(self):
        assert format_remote_path("/ccs/expr/{expname}", expname="N24L1") == "/ccs/expr/N24L1"

    def test_substitutes_obsdate_strftime_single_quotes(self):
        out = format_remote_path("vlbi_arch/{obsdate.strftime('%y%b')}",
                                 obsdate=dt.date(2024, 6, 12))
        assert out == "vlbi_arch/24Jun"

    def test_substitutes_obsdate_strftime_double_quotes(self):
        out = format_remote_path('vlbi_arch/{obsdate.strftime("%Y/%m/%d")}',
                                 obsdate=dt.date(2024, 6, 12))
        assert out == "vlbi_arch/2024/06/12"

    def test_substitutes_both_placeholders(self):
        out = format_remote_path(
            "/ccs/expr/{expname}/{obsdate.strftime('%y%b')}",
            expname="N24L1",
            obsdate=dt.date(2024, 6, 12),
        )
        assert out == "/ccs/expr/N24L1/24Jun"

    def test_passthrough_when_no_placeholders(self):
        assert format_remote_path("/data/exp/static", expname="X", obsdate=dt.date(2024, 1, 1)) \
            == "/data/exp/static"

    def test_unknown_placeholder_left_intact(self):
        # Anything we don't recognise must NOT be eval'd; it stays literal so an
        # eventual SCP/SSH call surfaces a clean "no such file" error.
        out = format_remote_path("/data/{unknown_field}/x", expname="X")
        assert out == "/data/{unknown_field}/x"

    def test_missing_obsdate_leaves_strftime_placeholder(self):
        # Caller forgot to pass obsdate: we don't crash, we leave the placeholder.
        out = format_remote_path("vlbi_arch/{obsdate.strftime('%y%b')}")
        assert "{obsdate.strftime" in out

    def test_missing_expname_leaves_placeholder(self):
        out = format_remote_path("/ccs/expr/{expname}")
        assert out == "/ccs/expr/{expname}"
