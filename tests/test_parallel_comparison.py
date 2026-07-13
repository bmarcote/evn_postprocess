"""Benchmark scaffold to compare parallelism approaches for intra-step work (Issue 9).

PRD open question 5 defers the choice of concurrency primitive (ThreadPoolExecutor vs
asyncio vs ...) to a later iteration. This harness runs the candidates over a synthetic
I/O-bound workload and reports comparative timings, so that later decision has a
reproducible measurement. It asserts only that every approach runs and returns the same
results -- never which one is faster (that would be flaky).
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor


def _work_item(n: int) -> int:
    """A small I/O-bound stand-in (a short sleep), mimicking a per-pass shell call."""
    time.sleep(0.01)
    return n * n


def _run_sequential(items):
    return [_work_item(n) for n in items]


def _run_threadpool(items, workers=4):
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(_work_item, items))


APPROACHES = {'sequential': _run_sequential, 'threadpool': _run_threadpool}


def compare(items) -> dict[str, tuple[list[int], float]]:
    """Runs every approach over *items*, returning {name: (results, seconds)}."""
    report: dict[str, tuple[list[int], float]] = {}
    for name, fn in APPROACHES.items():
        start = time.perf_counter()
        results = fn(items)
        report[name] = (results, time.perf_counter() - start)
    return report


def test_all_approaches_agree_and_report(capsys):
    items = list(range(8))
    expected = [n * n for n in items]
    report = compare(items)
    # Every approach exists and produces identical results.
    assert set(report) == set(APPROACHES)
    for name, (results, seconds) in report.items():
        assert results == expected
        assert seconds >= 0.0
        print(f"{name}: {seconds * 1000:.1f} ms")
    # Report is visible for the later decision (no assertion on which is fastest).
    out = capsys.readouterr().out
    assert 'threadpool' in out and 'sequential' in out
