from __future__ import annotations

import os
import re
import sys
import glob
import json
import socket
import signal
import subprocess
import collections
import mimetypes
import http.server
import threading
import datetime as dt
from importlib import resources
from operator import methodcaller
from functools import reduce, partial
from urllib.parse import unquote
from pathlib import Path
from rich import print as rprint
from typing import List, Optional, Generator
from loguru import logger
import numpy as np
from astropy import units as u
from casacore import tables as pt
from . import experiment  # cycle: experiment->process->plotting; module-form + future annotations
from . import experiment_state
from . import review
from .experiment_state import STATION_STATUSES
from jiveplot import jplotter, command  # noqa: F401  (re-exported for module callers)


# program default(s)
NoWgt = True    # do not produce weight plots
ScanNo = None    # automatic scan selection
Version = "$Id: standardplots,v 1.1 2014-08-08 15:38:41 jive_cc Exp $"
# Polarization colour scheme as per JIVE standard.
# Single pol data gets coloured black.
PolCMap = "ckey p[rr]=2 p[ll]=3 p[rl]=4 p[lr]=5 p[none]=1"

# function composition is really great
compose = lambda *fns: lambda x: reduce(lambda a, f: f(a), reversed(fns), x)
Map = lambda fn: partial(map, fn)

# full MS path to plot base name transformation
mk_basenm = compose(partial(re.sub, r'\.ms', ''), os.path.basename, partial(re.sub, r'/*$', ''))
# CalSrc must be transformed such that it can be fed into "/{0}/i" regex,
# even if CalSrc is comma-separated list of sources.
# So: split CalSrc by ',', escape the individual source names and transform
#     to "(<src>|<src>|....)" as regex alternatives for matching
mk_calsrc = compose("({0})".format, "|".join, Map(re.escape), methodcaller('split', ','))

# We need to capture errors and terminate in stead of going on.
# replace the errorfunction from hvutil with one that terminates
def mkerrf(pfx):
    def actualerrf(msg):
        print("{0} {1}".format(pfx, msg))
        sys.exit(-1)
    return actualerrf

jplotter.hvutil.mkerrf = mkerrf

def chunkert(f, l, cs, verbose=True):
    while f<l:
        n = min(cs, l-f)
        yield (f, n)
        f = f + n

# Default antenna priority for reference antenna fallback.
# Ordered by preference; the first antenna present in a scan with the most subbands wins.
DEFAULT_REFANT_PRIORITY = ('Ef', 'O8', 'Ys', 'Mc', 'Gb', 'At', 'Pt', 'Jb', 'Wb', 'Tr', 'Nt', 'Sv', 'Zc', 'Bd', 'Sh', 'Ur')


class Jplot:
    """Class for creating JIVE standard plots from Measurement Sets.

    Discovers which scans contain the requested calibrator sources, picks the
    best reference antenna per scan, and produces plots with the scan number
    embedded in every output filename so nothing is overwritten.
    """

    def __init__(self, ms: str, refant: str, calsrc: str, weight_plots: bool = False,
                 debug: bool = False, refant_priority: Optional[tuple[str, ...]] = None):
        """Initialize the Jplot instance.

        Args:
            ms: Path to the Measurement Set file.
            refant: Preferred reference antenna (two-letter station code).
            calsrc: Calibrator source(s), comma-separated.
            weight_plots: Whether to include weight plots (default: False).
            debug: Enable debug output (default: False).
            refant_priority: Ordered tuple of antenna codes used as fallback
                when *refant* is absent from a scan. Defaults to DEFAULT_REFANT_PRIORITY.
        """
        self.measurementset = ms
        self.refant = refant
        self.calsrc_raw = calsrc
        self.calsrc = mk_calsrc(calsrc)
        self.weight_plots = weight_plots
        self.debug = debug
        self.refant_priority = refant_priority or DEFAULT_REFANT_PRIORITY
        self.myBasename = mk_basenm(self.measurementset)
        self.tempFileName = "/tmp/sptf-{0}.ps".format(os.getpid())

        # Determine the best subband for time plots
        self.subbandNo = self._find_best_subband()
        print(f"Subband {self.subbandNo} selected for amp & phase VS time plot.")

    def cleanup(self):
        """Clean up temporary files."""
        try:
            os.unlink(self.tempFileName)
        except OSError:
            pass  # File might not exist

    def _find_best_subband(self) -> int:
        """Find the subband with most antenna coverage."""
        ants_spws = self._get_observed_subbands()
        counting: collections.Counter = collections.Counter()
        for antenna in ants_spws:
            counting.update(ants_spws[antenna])
        return counting.most_common()[0][0]

    def _get_observed_subbands(self) -> dict[str, set[int]]:
        """Get observed subbands for each antenna.

        Returns:
            dict mapping antenna name -> set of subband indices with non-zero data.
        """
        ants_spws: dict[str, set[int]] = collections.defaultdict(set)
        with pt.table(self.measurementset, readonly=True, ack=False) as mstable:
            with pt.table(mstable.getkeyword('ANTENNA'), readonly=True, ack=False) as ms_ants:
                antenna_names = ms_ants.getcol('NAME')

            with pt.table(mstable.getkeyword('DATA_DESCRIPTION'), readonly=True, ack=False) as ms_spws:
                spw_names = ms_spws.getcol('SPECTRAL_WINDOW_ID')

            for (start, nrow) in chunkert(0, len(mstable), 5000):
                ants1 = mstable.getcol('ANTENNA1', startrow=start, nrow=nrow)
                ants2 = mstable.getcol('ANTENNA2', startrow=start, nrow=nrow)
                spws = mstable.getcol('DATA_DESC_ID', startrow=start, nrow=nrow)
                msdata = mstable.getcol('DATA', startrow=start, nrow=nrow)

                for antenna in antenna_names:
                    for spw in spw_names:
                        if (msdata[np.where((ants1 == antenna) & (ants2 == antenna) & (spws == spw))] < 1e-7).all():
                            ants_spws[antenna].add(spw)

        return ants_spws

    # ------------------------------------------------------------------
    #  Scan discovery & reference-antenna selection
    # ------------------------------------------------------------------

    def get_scans_for_sources(self, sources: list[str]) -> dict[int, dict]:
        """Find all scans that contain any of the requested sources.

        Reads SCAN_NUMBER, FIELD_ID and ANTENNA1/2 columns to build a map
        of scan_number -> {source, antennas, antenna_spws}.

        Args:
            sources: List of source names to match.

        Returns:
            dict mapping scan_number (int) -> {
                'source':       str,          # field name for this scan
                'antennas':     set[str],     # antenna names with data
                'antenna_spws': dict[str, set[int]]  # per-antenna subband set
            }
        """
        source_set = {s.upper() for s in sources}
        result: dict[int, dict] = {}

        with pt.table(self.measurementset, readonly=True, ack=False) as mstable:
            with pt.table(mstable.getkeyword('ANTENNA'), readonly=True, ack=False) as ant_tab:
                ant_names = list(ant_tab.getcol('NAME'))

            with pt.table(mstable.getkeyword('FIELD'), readonly=True, ack=False) as field_tab:
                field_names = list(field_tab.getcol('NAME'))

            for (start, nrow) in chunkert(0, len(mstable), 5000, verbose=False):
                scan_col = mstable.getcol('SCAN_NUMBER', startrow=start, nrow=nrow)
                field_col = mstable.getcol('FIELD_ID', startrow=start, nrow=nrow)
                ant1_col = mstable.getcol('ANTENNA1', startrow=start, nrow=nrow)
                ant2_col = mstable.getcol('ANTENNA2', startrow=start, nrow=nrow)
                spw_col = mstable.getcol('DATA_DESC_ID', startrow=start, nrow=nrow)
                data_col = mstable.getcol('DATA', startrow=start, nrow=nrow)

                for i in range(nrow):
                    fid = int(field_col[i])
                    fname = field_names[fid] if fid < len(field_names) else ''
                    if fname.upper() not in source_set:
                        continue

                    scanno = int(scan_col[i])
                    if scanno not in result:
                        result[scanno] = {'source': fname, 'antennas': set(), 'antenna_spws': collections.defaultdict(set)}

                    a1 = int(ant1_col[i])

                    if a1 == int(ant2_col[i]) and np.max(np.abs(data_col[i])) > 1e-5:
                        aname = ant_names[a1]
                        result[scanno]['antennas'].add(aname)
                        result[scanno]['antenna_spws'][aname].add(int(spw_col[i]))

        return dict(sorted(result.items()))

    def pick_refant_for_scan(self, scan_info: dict) -> str:
        """Choose the best reference antenna for a single scan.

        If the preferred refant is present in the scan it is returned directly.
        Otherwise the antenna from the priority list that observed the most
        subbands in this scan is selected.

        Args:
            scan_info: Dict with 'antennas' (set[str]) and
                       'antenna_spws' (dict[str, set[int]]) as returned by
                       get_scans_for_sources().

        Returns:
            Two-letter antenna code to use as reference antenna.

        Raises:
            ValueError: If no suitable reference antenna can be found.
        """
        if self.refant in scan_info['antennas']:
            return self.refant

        # Build candidates from the priority list that are present in this scan
        candidates = [a for a in self.refant_priority if a in scan_info['antennas']]
        if not candidates:
            # Fall back to any antenna present, sorted by subband count descending
            candidates = list(scan_info['antennas'])

        if not candidates:
            raise ValueError("No antennas found in scan to use as reference antenna")

        # Pick the candidate with the most subbands
        ant_spws = scan_info['antenna_spws']
        best = max(candidates, key=lambda a: len(ant_spws.get(a, set())))
        print(f"  refant fallback: {self.refant} not in scan, using {best} "
              f"({len(ant_spws.get(best, set()))} subbands)")
        return best

    def open_ms(self) -> Generator[str, None, None]:
        """Open MS and run indexr - returns generator of jplotter commands."""
        yield "ms {0}".format(self.measurementset)
        yield "indexr"
        yield "refile {0}".format(self.tempFileName)

    # ------------------------------------------------------------------
    #  Plot generators (each yields jplotter command strings)
    # ------------------------------------------------------------------

    def anp_chan_cross_plot(self, refant: str, scanno: int) -> Generator[str, None, None]:
        """Generate amplitude/phase vs channel cross-baseline plots for one scan.

        Args:
            refant: Reference antenna code for this scan.
            scanno: Scan number to select.

        Returns:
            Generator of jplotter commands.
        """
        print(f"generating cross plots [anp/channel] scan {scanno}")
        yield "bl {0}* -auto".format(refant)
        yield "fq *;ch none"
        yield "avt vector;avc none"
        yield "pt anpchan"
        yield "y local"
        yield "scan mid-30s to mid+30s where scan_number={0}".format(scanno)
        yield "new all false bl true sb false"
        yield "multi true"
        yield "sort bl"
        yield PolCMap
        yield "nxy 2 4"
        yield "refile {0}-cross-scan{1}.ps/cps".format(self.myBasename, scanno)
        yield "pl"
        print(f"done cross plots scan {scanno}")

    def amp_chan_auto_plot(self, scanno: int) -> Generator[str, None, None]:
        """Generate amplitude vs channel auto-correlation plots for one scan.

        Args:
            scanno: Scan number to select.

        Returns:
            Generator of jplotter commands.
        """
        print(f"generating auto plots [amp/channel] scan {scanno}")
        yield "bl auto"
        yield "fq */p;ch none"
        yield "avt scalar;avc none"
        yield "time none"
        yield "pt ampchan"
        yield "y 0 2"
        yield "scan mid-30s to mid+30s where scan_number={0}".format(scanno)
        yield "new all false bl true sb false time true"
        yield "multi true"
        yield "sort bl"
        yield PolCMap
        yield "nxy 2 4"
        yield "refile {0}-auto-scan{1}.ps/cps".format(self.myBasename, scanno)
        yield "pl"
        print(f"done auto plots scan {scanno}")

    def amp_time_auto_plot(self, scanno: int) -> Generator[str, None, None]:
        """Generate amplitude vs time auto-correlation plots for one scan.

        Args:
            scanno: Scan number to select.

        Returns:
            Generator of jplotter commands.
        """
        print(f"generating auto plots [amp/time] scan {scanno}")
        yield "bl auto"
        yield "fq *;ch 0.1*last:0.9*last"
        yield "new all f bl t"
        yield "avt none;avc vector"
        yield "pt amptime"
        yield "y local"
        yield "scan start-20m to end+100m where scan_number={0}".format(scanno)
        yield "time"
        yield "sort bl"
        yield "refile {0}-amptime-scan{1}.ps/cps".format(self.myBasename, scanno)
        yield "pl"
        print(f"done amp/time auto plots scan {scanno}")

    def anp_time_cross_plot(self, refant: str, timesel: str, sbsel: str, label: str) -> Generator[str, None, None]:
        """Generate amplitude/phase vs time cross-baseline plots (all scans).

        Args:
            refant: Reference antenna code.
            timesel: Time selection string.
            sbsel: Subband selection string.
            label: Suffix label for output filename.

        Returns:
            Generator of jplotter commands.
        """
        print(f"generating cross plots [anp/time] all scans ({label})")
        yield "bl {0}* -auto".format(refant)
        yield "fq {0}/p;ch 0.1*last:0.9*last".format(sbsel)
        yield "new all f bl t"
        yield "avt none;avc vector"
        yield "pt anptime"
        yield "y local"
        yield "src none"
        yield "time {0}".format(timesel)
        yield "sort bl"
        yield "ckey src src[none]=1"
        yield "ptsz 2"
        yield "refile {0}-ampphase-{1}.ps/cps".format(self.myBasename, label)
        yield "pl"
        print(f"done amplitude/phase vs time plots ({label})")

    def weight_plot(self) -> Generator[str, None, None]:
        """Generate weight plots for auto-correlations.

        Returns:
            Generator of jplotter commands.
        """
        print("generating weight plot")
        yield "ms {0}".format(self.measurementset)
        yield "bl auto; fq */p"
        yield "src none"
        yield "time none"
        yield "ch mid"
        yield "pt wt"
        yield "new all f bl t"
        yield "y global"
        yield "sort bl"
        yield "refile {0}-weight.ps/cps".format(self.myBasename)
        yield "wt 0.1"
        yield "pl"
        print("done weight plot")


    def create_plot(self, sources: list[str], plots: Optional[List[str]] = None) -> bool:
        """Discover scans for the given sources and create plots for each scan.

        For every scan that contains one of *sources*, cross- and auto-correlation
        plots are generated with the scan number in the filename.  The reference
        antenna is chosen per-scan (falls back through the priority list when the
        preferred refant is absent).

        Amplitude/phase-vs-time and weight plots are produced once (not per-scan).

        Args:
            sources: Calibrator source names to look for in the MS.
            plots: Plot types to create. If None, creates all standard plots.
                   Available options: 'cross', 'auto', 'time', 'weight'.

        Returns:
            True if successful, False otherwise.
        """
        if not sources:
            print("ERROR: No calibrator sources provided for plotting.")
            return False

        try:
            todo: list[Generator[str, None, None]] = [self.open_ms()]

            if plots is None:
                plots = ['cross', 'auto', 'time']
                if self.weight_plots:
                    plots.append('weight')

            # --- discover scans containing the requested sources ---
            scan_map = self.get_scans_for_sources(sources)
            if not scan_map:
                print(f"WARNING: No scans found for sources {sources}. Skipping per-scan plots.")
            else:
                print(f"Found {len(scan_map)} scans for sources {sources}: "
                      f"{list(scan_map.keys())}")

            # --- per-scan plots (cross + auto) ---
            for scanno, info in scan_map.items():
                if 'cross' in plots:
                    todo.append(self.anp_chan_cross_plot(self.pick_refant_for_scan(info), scanno))
                if 'auto' in plots:
                    todo.append(self.amp_chan_auto_plot(scanno))

            # --- amplitude/phase vs time (full observation, not per-scan) ---
            if 'time' in plots:
                for i, time_sel in enumerate(('none', '$start-5m to +55m')):
                    todo.append(self.anp_time_cross_plot(self.refant, time_sel,
                                                        str(self.subbandNo), str(i)))

            # --- weight plots ---
            if 'weight' in plots and self.weight_plots:
                todo.append(self.weight_plot())

            # --- info ---
            todo.append(self._info_command())

            jplotter.run_plotter(command.scripted(*todo), debug=self.debug)
            return True

        except Exception as e:
            print(f"Error during plotting: {e}")
            return False
        finally:
            self.cleanup()

    def _info_command(self) -> Generator[str, None, None]:
        """Generate MS info command."""
        yield "r"


def convert_ps_to_png(plots_dir: Path, expname: str, resolution: int = 150) -> list[Path]:
    """Convert all PostScript (.ps) plot files to PNG format using Ghostscript.

    Multi-page PS files produce one PNG per page, named with a ``-pageNNN`` suffix.
    Single-page PS files produce a single PNG without the suffix.

    Args:
        plots_dir: Destination directory for the PNG files (typically Dirs.plots).
        expname: Experiment name (lowercase) used to glob matching .ps files.
        resolution: DPI resolution for the output PNGs. Default 150.

    Returns:
        List of Path objects for the created PNG files.
    """
    plots_dir.mkdir(parents=True, exist_ok=True)
    ps_files = sorted(glob.glob(f"{expname}*.ps"))
    if not ps_files:
        logger.warning(f"No .ps files found matching '{expname}*.ps'")
        return []

    created: list[Path] = []
    for ps_file in ps_files:
        stem = Path(ps_file).stem
        # Skip conversion if up-to-date PNG(s) already exist (newer than the .ps file).
        # This avoids needlessly re-rendering every plot each time the dashboard is opened
        # (e.g. when re-running the msops step without having regenerated the plots).
        ps_mtime = os.path.getmtime(ps_file)
        existing = sorted(plots_dir.glob(f"{stem}.png")) or sorted(plots_dir.glob(f"{stem}-page*.png"))
        if existing and all(p.stat().st_mtime >= ps_mtime for p in existing):
            logger.debug(f"PNG(s) for {ps_file} already up to date; skipping conversion.")
            created.extend(existing)
            continue
        # Use %03d placeholder so Ghostscript writes one PNG per page (1-based)
        cmd = ["gs", "-dBATCH", "-dNOPAUSE", "-dSAFER", "-sDEVICE=png16m",
               f"-r{resolution}", f"-sOutputFile={plots_dir / f'{stem}-page%03d.png'}", str(ps_file)]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode == 0:
                page_files = sorted(plots_dir.glob(f"{stem}-page*.png"))
                if len(page_files) == 1:
                    # Single-page PS: rename to drop the -page001 suffix
                    clean_path = plots_dir / f"{stem}.png"
                    page_files[0].rename(clean_path)
                    page_files = [clean_path]
                logger.info(f"Converted {ps_file} -> {len(page_files)} page(s)")
                created.extend(page_files)
            else:
                logger.error(f"Ghostscript failed for {ps_file}: {result.stderr.strip()}")
        except FileNotFoundError:
            logger.error("Ghostscript (gs) not found. Install it to convert PS to PNG.")
            break
        except subprocess.TimeoutExpired:
            logger.error(f"Ghostscript timed out converting {ps_file}")

    return created


def _find_available_port(start: int = 8050, end: int = 8150) -> int:
    """Find an available TCP port in the given range.

    Args:
        start: First port to try.
        end: Last port to try (exclusive).

    Returns:
        An available port number.

    Raises:
        RuntimeError: If no port in the range is available.
    """
    for port in range(start, end):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No available port found in range {start}-{end}")


def _format_scan_timerange(scan) -> str:
    """Return the observing time range of *scan*, as scheduled in the vex $SCHED block.

    Used as the tooltip of every row of the dashboard scan-overview table. The end time
    repeats the date only when the scan crosses midnight, and a scan whose duration could
    not be parsed from the vex file (duration_s == 0) shows its start time alone.

    Args:
        scan: experiment.Scan object (starttime + duration_s come straight from the vex).

    Returns:
        e.g. "21/05/2024 10:23:00-10:29:00 UTC (6.0 min)"; "" if the scan has no start time.
    """
    if scan.starttime is None:
        return ""
    start_str = scan.starttime.strftime('%d/%m/%Y %H:%M:%S')
    if not scan.duration_s:
        return f"{start_str} UTC"
    end = scan.starttime + dt.timedelta(seconds=scan.duration_s)
    end_str = end.strftime('%H:%M:%S' if end.date() == scan.starttime.date() else '%d/%m/%Y %H:%M:%S')
    duration = f"{scan.duration_s} s" if scan.duration_s < 60 else f"{scan.duration_s / 60:.1f} min"
    return f"{start_str}-{end_str} UTC ({duration})"


def _build_experiment_summary(exp) -> dict:
    """Extract experiment metadata into a plain dict for the dashboard JSON API.

    Args:
        exp: experiment.Experiment object.

    Returns:
        dict with keys suitable for JSON serialization.
    """
    summary: dict = {
        "expname": exp.expname,
        "obsdate": exp.obsdate.strftime("%d/%m/%Y") if exp.obsdate else "Unknown",
        "timerange": (f"{exp.timerange[0].strftime('%H:%M')}-{exp.timerange[1].strftime('%H:%M')} UTC"
                      if exp.timerange else ""),
        "eEVNname": exp.eEVNname,
        "supsci": exp.supsci,
        "pi": [{"name": p.name, "email": p.email} for p in exp.pi] if exp.pi else [],
        "credentials": {"username": exp.credentials.username, "password": exp.credentials.password}
                        if exp.credentials else None,
        "refant": exp.refant,
        "feedback_page": exp.feedback_page(),
        "archive_page": exp.archive_page,
    }

    # Sources and per-source type lookup (used to color scan rows)
    summary["sources"] = {
        "fringefinder": exp.sources.fringefinder,
        "target": exp.sources.target,
        "calibrator": exp.sources.calibrator,
    }
    source_type_map: dict[str, str] = {}
    for src in exp.sources:
        source_type_map[src.name] = src.type.name
    summary["source_types"] = source_type_map

    # Antennas
    summary["antennas"] = {
        "observed": [a.name for a in exp.antennas if a.observed],
        "not_observed": [a.name for a in exp.antennas if not a.observed],
        "polswap": exp.antennas.polswap,
        "polconvert": exp.antennas.polconvert,
        "onebit": exp.antennas.onebit,
    }

    # Correlator passes (freq setup)
    passes = []
    for i, cp in enumerate(exp.correlator_passes):
        p: dict = {"index": i, "lisfile": str(cp.lisfile), "msfile": str(cp.msfile),
                   "fitsidi": cp.fitsidifile if cp.fitsidifile else ""}
        if cp.freqsetup:
            p["frequency"] = f"{cp.freqsetup.frequency.to(u.GHz):0.04}"
            p["bandwidth"] = f"{cp.freqsetup.bandwidth.to(u.MHz):0.04}"
            p["subbands"] = int(cp.freqsetup.subbands)
            p["channels"] = int(cp.freqsetup.channels)
        if cp.flagged_weights is not None:
            p["flag_threshold"] = cp.flagged_weights.threshold
            p["flag_percentage"] = cp.flagged_weights.percentage
        passes.append(p)
    summary["correlator_passes"] = passes
    # Automatic polarization diagnostics (polswap/polconvert findings from the lag MS).
    summary["pol_diagnostics"] = exp.pol_diagnostics if hasattr(exp, 'pol_diagnostics') else {}

    # Scans overview
    all_antennas = sorted(exp.antennas.names)
    scans_overview: list[dict] = []
    for scan in exp.scans:
        scheduled = set(scan.stations_scheduled)
        observed = set(scan.stations_observed)
        row: dict = {"scanno": scan.scanno, "source": scan.source, "antennas": {},
                     "timerange": _format_scan_timerange(scan)}
        for ant in all_antennas:
            if ant in scheduled:
                row["antennas"][ant] = "observed" if ant in observed else "missing"
            else:
                row["antennas"][ant] = "not_scheduled"
        scans_overview.append(row)
    summary["scans"] = scans_overview
    summary["all_antennas"] = all_antennas
    summary["lag_snr"] = exp.lag_snr if hasattr(exp, 'lag_snr') else {}

    return summary


# Dashboard page skeleton (HTML + CSS + JS). A plain text file so it can be edited
# without touching Python; see _build_dashboard_html for the placeholder contract.
_DASHBOARD_TEMPLATE = "dashboard.html.template"
_DASHBOARD_PLACEHOLDER_RE = re.compile(r"\{\{[A-Z_]+\}\}")


def _build_dashboard_html(exp) -> str:
    """Return the full HTML/CSS/JS for the experiment dashboard single-page app.

    The page skeleton lives in ``templates/dashboard.html.template`` and is read from
    there at serve time, so its markup, CSS and JS can be edited as ordinary text
    without touching this module.

    Placeholders in that file use double braces (``{{EXPNAME}}``) instead of the single
    braces of the other templates: the file is full of JavaScript that uses ``{...}``
    blocks and ``${...}`` template literals, and only the exact tokens listed in
    *replacements* below are substituted, so the JS is never touched.

    Everything else the page shows is fetched at runtime from /api/summary and
    /api/plots; the placeholders cover only what must already be right before the first
    fetch returns (the browser tab title and the header).

    Args:
        exp: experiment.Experiment object supplying the placeholder values.

    Returns:
        HTML string with every placeholder replaced.

    Raises:
        RuntimeError: if the template still holds an unknown ``{{NAME}}`` placeholder, or
            no longer mentions every experiment_state.STATION_STATUSES value.
    """
    html = resources.files("evn_postprocess.templates").joinpath(_DASHBOARD_TEMPLATE).read_text(encoding="utf-8")
    replacements = {"{{EXPNAME}}": exp.expname}
    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)

    # An externally edited template must fail loudly here rather than serve a page that
    # shows a raw '{{NAME}}' to the operator.
    if unknown := _DASHBOARD_PLACEHOLDER_RE.findall(html):
        raise RuntimeError(f"{_DASHBOARD_TEMPLATE} contains unknown placeholder(s): "
                           f"{', '.join(sorted(set(unknown)))}. Known placeholders: "
                           f"{', '.join(replacements)}.")

    # Guard against vocabulary drift: the JS in the template hand-codes the station
    # statuses (STATUS_COLORS / option labels); they must match STATION_STATUSES.
    for status in STATION_STATUSES:
        if f"'{status}'" not in html:
            raise RuntimeError(f"{_DASHBOARD_TEMPLATE} is missing station status '{status}': "
                               "update the Comments-tab JS to match "
                               "experiment_state.STATION_STATUSES.")
    return html


class _DashboardHandler(http.server.BaseHTTPRequestHandler):
    """HTTP request handler for the experiment dashboard.

    Class attributes (set before serving):
        experiment_summary: dict with experiment metadata.
        plots_dir: Path to the directory containing PNG plot files.
        expname: Experiment name (lowercase) for filtering plot files.
        dashboard_html: Pre-built HTML string for the dashboard page.
    """
    experiment_summary: dict = {}
    plots_dir: Path = Path("plots")
    expname: str = ""
    dashboard_html: str = ""
    exp: experiment.Experiment | None = None
    # Pipeline feedback page(s): directory holding the {expname}*.html feedback page(s)
    # and their linked products, and the list of page filenames. Empty/None until the
    # pipeline feedback has been generated (see serve_dashboard's pipeline_dir argument).
    pipeline_dir: Optional[Path] = None
    pipeline_pages: List[str] = []

    def do_GET(self):
        """Route GET requests to the appropriate handler."""
        try:
            if self.path == "/" or self.path == "/index.html":
                self._serve_html()
            elif self.path == "/api/summary":
                self._serve_json(self.experiment_summary)
            elif self.path == "/api/plots":
                self._serve_plot_list()
            elif self.path == "/api/pipeline":
                self._serve_json(self.pipeline_pages)
            elif self.path == "/api/comments":
                self._serve_comments()
            elif self.path.startswith("/plots/"):
                self._serve_plot_file()
            elif self.path.startswith("/pipeline/"):
                self._serve_pipeline_file()
            else:
                self.send_error(404)
        except Exception as exc:
            logger.error(f"Dashboard GET {self.path} failed: {exc}")
            body = json.dumps({"error": str(exc)}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def do_POST(self):
        """Route POST requests to the appropriate handler."""
        if self.path == "/api/set_source_type":
            self._handle_set_source_type()
        elif self.path == "/api/set_refant":
            self._handle_set_refant()
        elif self.path == "/api/set_comments":
            self._handle_set_comments()
        else:
            self.send_error(404)

    # Cache of the auto-generated default station comments (computed once per server
    # run: it may query the feedback database). None means "not computed yet".
    _default_comments: Optional[dict] = None

    def _experiment(self) -> experiment.Experiment:
        """Returns the served Experiment, set once by serve_dashboard before the HTTP
        server starts accepting requests; never None while a request is being handled.
        """
        if self.__class__.exp is None:
            raise RuntimeError("_DashboardHandler.exp is not set; serve_dashboard() must run first.")
        return self.__class__.exp

    def _exp_toml(self):
        """Returns the experiment toml of the served experiment, loading it if needed."""
        return experiment_state.attached_toml(self._experiment())

    def _serve_comments(self):
        """GET /api/comments: general note + per-station comments for the Comments tab.

        Saved toml [comments] entries win; stations without a saved entry get the
        auto-generated defaults (station-summary findings + feedback-DB comment).
        """
        exp_toml = self._exp_toml()
        if self.__class__._default_comments is None:
            try:
                self.__class__._default_comments = review.default_station_comments(self._experiment())
            except Exception as exc:  # defaults must never break the dashboard
                logger.warning(f"Could not compute the default station comments: {exc}")
                self.__class__._default_comments = {}
        stations = {}
        for name, default in self.__class__._default_comments.items():
            saved = exp_toml.comments.stations.get(name)
            stations[name] = ({'status': saved.status, 'note': saved.note} if saved is not None
                              else dict(default))
        for name, saved in exp_toml.comments.stations.items():
            stations.setdefault(name, {'status': saved.status, 'note': saved.note})
        self._serve_json({'general': exp_toml.comments.general, 'stations': stations})

    def _handle_set_comments(self):
        """POST /api/set_comments: persists the Comments tab into the experiment toml.

        Expects JSON body: {"general": str, "stations": {NAME: {"status": s, "note": n}}}.
        """
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            general = body.get("general")
            stations = body.get("stations", {})
        except (json.JSONDecodeError, ValueError) as exc:
            self._serve_json({"ok": False, "error": str(exc)})
            return
        try:
            # Reload from disk before writing: the paused workflow process may have
            # recorded parameters since this server loaded the toml (lost-update guard).
            exp_toml = experiment_state.attached_toml(self._experiment(), fresh=True)
            exp_toml.record_comments(general=general, stations=stations)
            exp_toml.save()
        except (experiment_state.ExperimentTomlError, OSError) as exc:
            self._serve_json({"ok": False, "error": str(exc)})
            return
        logger.info(f"Experiment comments saved to {exp_toml.path}.")
        self._serve_json({"ok": True})

    def _handle_set_source_type(self):
        """Change a source's type and persist via exp.store().

        Expects JSON body: {"source": "NAME", "type": "target|calibrator|fringefinder|other"}
        Updates the exp object in-place, rebuilds the summary dict, and stores to disk.
        """
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            src_name = body["source"]
            new_type_name = body["type"]
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            self._serve_json({"ok": False, "error": str(exc)})
            return

        exp = self._experiment()
        # Find the source and get the SourceType enum class from it
        target_src = None
        for src in exp.sources:
            if src.name == src_name:
                target_src = src
                break

        if target_src is None:
            self._serve_json({"ok": False, "error": f"Source '{src_name}' not found"})
            return

        source_type_cls = type(target_src.type)  # the SourceType enum class
        try:
            target_src.type = source_type_cls[new_type_name]
        except KeyError:
            valid = [e.name for e in source_type_cls]
            self._serve_json({"ok": False, "error": f"Invalid type '{new_type_name}'. Valid: {valid}"})
            return

        exp.store()
        self.__class__.experiment_summary = _build_experiment_summary(exp)
        logger.info(f"Source '{src_name}' type changed to '{new_type_name}' and stored.")
        self._serve_json({"ok": True})

    def _handle_set_refant(self):
        """Change the primary reference antenna and persist via exp.store().

        Expects JSON body: {"refant": "AntName"}. The chosen antenna becomes the
        first element of exp.refant (the primary used everywhere); any other
        previously-listed refants are kept after it as fallback order.
        """
        try:
            new_ref = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))["refant"]
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            self._serve_json({"ok": False, "error": str(exc)})
            return

        exp = self._experiment()
        if new_ref not in exp.antennas.names:
            self._serve_json({"ok": False, "error": f"Antenna '{new_ref}' not in this experiment"})
            return

        # Put the chosen antenna first, keeping the rest of the previous list as fallback.
        exp.refant = [new_ref] + [r for r in exp.refant if r != new_ref]
        exp.store()
        self.__class__.experiment_summary = _build_experiment_summary(exp)
        logger.info(f"Reference antenna set to '{new_ref}' (refant order: {', '.join(exp.refant)}) and stored.")
        self._serve_json({"ok": True})

    def _serve_html(self):
        """Serve the dashboard HTML page."""
        content = self.dashboard_html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _serve_json(self, data: dict | list):
        """Serve a JSON response."""
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_plot_list(self):
        """Return a JSON list of available PNG plot filenames."""
        self._serve_json(sorted(f.name for f in self.plots_dir.glob(f"{self.expname}*.png")))

    def _serve_plot_file(self):
        """Serve a PNG plot image file."""
        filename = self.path.split("/plots/", 1)[-1]
        # Sanitize to prevent path traversal
        filename = Path(filename).name
        filepath = self.plots_dir / filename
        if not filepath.exists() or not filepath.is_file():
            self.send_error(404)
            return
        data = filepath.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "image/png")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_pipeline_file(self):
        """Serve a pipeline feedback file (HTML page or a linked product) from pipeline_dir.

        Handles the feedback page itself plus every product it links to (PDF, PNG, FITS,
        TXT, DTSUM, SCAN, ...). The content type is guessed from the suffix, defaulting to
        plain text for the textual products and octet-stream (download) otherwise.
        """
        if self.pipeline_dir is None:
            self.send_error(404)
            return
        rel = unquote(self.path.split("/pipeline/", 1)[-1])
        filename = Path(rel).name  # collapse any path components to prevent traversal
        if not filename:
            self.send_error(404)
            return
        filepath = self.pipeline_dir / filename
        if not filepath.exists() or not filepath.is_file():
            self.send_error(404)
            return
        ctype, _ = mimetypes.guess_type(str(filepath))
        if ctype is None:
            # Products without a registered MIME type: serve the textual ones inline,
            # everything else (e.g. .FITS) as a download.
            ctype = ("text/plain" if filepath.suffix.lower() in (".txt", ".ampcal", ".dtsum", ".scan")
                     else "application/octet-stream")
        data = filepath.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format, *args):
        """Suppress routine access logs but let errors through via loguru."""
        pass

    def log_error(self, format, *args):
        """Forward HTTP errors to loguru so they are visible in the terminal."""
        logger.error(f"Dashboard HTTP error: {format % args}")


def serve_dashboard(exp, plots_dir: Path, pipeline_dir: Optional[Path] = None) -> None:
    """Start an HTTP dashboard server showing experiment summary and standard plots.

    Converts any .ps files to PNG (if not already done), then serves a web dashboard
    with the experiment overview (same info as print_blessed), scan overview table,
    and a plot viewer with selectors for plot type and scan number.

    The server picks the first available port in the 8050-8150 range and runs
    until the user interrupts with Ctrl+C.

    Args:
        exp: experiment.Experiment object with all metadata populated.
        plots_dir: Path to the directory where PNG plot files are stored (Dirs.plots).
        pipeline_dir: Optional path to the directory holding the pipeline feedback
            page(s) (``{expname}*.html``) and their linked products (normally
            ``Dirs.pipe_out``). When given and at least one feedback page is found, the
            dashboard shows a "Pipeline" tab (selected by default, on top of the standard
            plots). When None or no page exists, only the standard plots are shown.
    """
    # Ensure PNGs exist
    if not list(plots_dir.glob(f"{exp.expname.lower()}*.png")):
        logger.info("Converting PostScript plots to PNG for the dashboard...")
        convert_ps_to_png(plots_dir, exp.expname.lower())

    # Discover pipeline feedback page(s), if any.
    pipeline_pages: List[str] = []
    if pipeline_dir is not None and pipeline_dir.exists():
        pipeline_pages = sorted(p.name for p in pipeline_dir.glob(f"{exp.expname.lower()}*.html"))

    port = _find_available_port()

    # Configure the handler class
    _DashboardHandler.exp = exp
    _DashboardHandler.experiment_summary = _build_experiment_summary(exp)
    _DashboardHandler.plots_dir = plots_dir
    _DashboardHandler.expname = exp.expname.lower()
    _DashboardHandler.dashboard_html = _build_dashboard_html(exp)
    _DashboardHandler.pipeline_dir = pipeline_dir if pipeline_pages else None
    _DashboardHandler.pipeline_pages = pipeline_pages
    # Reset the per-experiment cache: a second serve_dashboard call in the same
    # process must not show the previous experiment's default station comments.
    _DashboardHandler._default_comments = None

    # Bind localhost only: the dashboard exposes unauthenticated write endpoints
    # (comments, source types, refant), so it must not be reachable from the network.
    # Remote viewing goes through the SSH tunnel whose command is printed below.
    server = http.server.HTTPServer(("127.0.0.1", port), _DashboardHandler)
    url = f"http://localhost:{port}"
    rprint(f"[green]\n{'=' * 60}[/green]")
    rprint(f"[green]  EVN Dashboard for {exp.expname} running at:[/green]")
    rprint(f"[bold green]  {url}[/bold green]")
    rprint("[bold green]Create a tunnel to open it in your browser with "
           f"'ssh -L {port}:localhost:{port} {exp.supsci.lower()}@eee2'[/bold green]")
    rprint("[green]  Press Ctrl+C to stop the server.\n[/green]")
    rprint(f"[green]{'=' * 60}[/green]")

    # Handle Ctrl+C gracefully
    original_sigint = signal.getsignal(signal.SIGINT)

    def _shutdown(signum, frame):
        print("\nShutting down dashboard server...")
        threading.Thread(target=server.shutdown).start()

    signal.signal(signal.SIGINT, _shutdown)
    try:
        server.serve_forever()
    finally:
        signal.signal(signal.SIGINT, original_sigint)
        server.server_close()
        logger.info("Dashboard server stopped.")


# Example usage at the bottom of the file:
if __name__ == "__main__":
    # Example: Create all standard plots for a fringe-finder
    plotter = Jplot("experiment.ms", "Ef", "J0613+5209", weight_plots=True)
    success = plotter.create_plot(sources=["J0613+5209"])

    # Example: Create only specific plot types
    # plotter = Jplot("experiment.ms", "Ef", "J0613+5209")
    # success = plotter.create_plot(sources=["J0613+5209"], plots=['cross', 'time'])

    print(f"Plotting {'succeeded' if success else 'failed'}")
