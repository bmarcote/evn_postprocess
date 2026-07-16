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

try:
    from jiveplot import jplotter, command  # noqa: F401  (re-exported for module callers)
    _JIVEPLOT_AVAILABLE = True
except ModuleNotFoundError:
    # jiveplot is only required for standardplots / web-dashboard rendering.
    # Defer the failure so the rest of the package (and the tests) can import without it.
    jplotter = None
    command = None
    _JIVEPLOT_AVAILABLE = False


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
if _JIVEPLOT_AVAILABLE:
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
        row: dict = {"scanno": scan.scanno, "source": scan.source, "antennas": {}}
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


def _build_dashboard_html() -> str:
    """Return the full HTML/CSS/JS for the experiment dashboard single-page app.

    The page fetches /api/summary and /api/plots from the embedded HTTP server,
    then renders an experiment overview on the left column and a plot viewer with
    selectors on the right column.

    Returns:
        HTML string.
    """
    html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>EVN Post-Processing Dashboard</title>
<style>
  :root { --bg: #1e1e2e; --surface: #2a2a3c; --border: #3a3a4c; --text: #e0e0e8;
           --accent: #7c8dff; --green: #50c878; --red: #ff6b6b; --dim: #888; --header-bg: #252538; }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); }
  header { background: var(--header-bg); padding: 1rem 2rem; border-bottom: 2px solid var(--accent);
           display: flex; align-items: center; gap: 1rem; }
  header h1 { font-size: 1.4rem; font-weight: 600; }
  header h1 span { color: var(--accent); }
  .container { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; padding: 1rem; height: calc(100vh - 60px); }
  .panel { background: var(--surface); border-radius: 8px; border: 1px solid var(--border);
           overflow-y: auto; padding: 1rem; }
  .panel h2 { font-size: 1.1rem; color: var(--accent); margin-bottom: 0.8rem; border-bottom: 1px solid var(--border); padding-bottom: 0.4rem; }
  .info-grid { display: grid; grid-template-columns: auto 1fr; gap: 0.3rem 1rem; font-size: 0.9rem; }
  .info-grid .label { color: var(--dim); font-weight: 500; white-space: nowrap; }
  .info-grid .value { word-break: break-all; }
  .info-grid .value a { color: var(--accent); text-decoration: none; }
  .section { margin-top: 1rem; }
  .tag { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; margin: 2px; }
  .tag-green { background: #1a3a2a; color: var(--green); }
  .tag-red { background: #3a1a1a; color: var(--red); }
  .tag-blue { background: #1a2a3a; color: var(--accent); }
  .tag-dim { background: #2a2a2a; color: var(--dim); }
  /* Scan table */
  .scan-table-wrap { overflow-x: auto; max-height: 400px; overflow-y: auto; margin-top: 0.5rem; }
  .scan-table { border-collapse: collapse; font-size: 0.75rem; width: 100%; }
  .scan-table th, .scan-table td { padding: 3px 6px; border: 1px solid var(--border); text-align: center; white-space: nowrap; }
  .scan-table th { background: var(--header-bg); position: sticky; top: 0; z-index: 1; }
  .scan-table .cell-obs { background: #1a4a2a; color: var(--green); font-weight: bold; }
  .scan-table .cell-warn { background: #4a3a1a; color: #f0c040; font-weight: bold; }
  .scan-table .cell-miss { background: #4a1a1a; color: var(--red); font-weight: bold; }
  .scan-table .cell-na { color: var(--border); }
  .src-fringefinder { color: #48dbfb; font-weight: 600; }
  .src-target { color: #ff9f43; font-weight: 600; }
  .src-calibrator { color: #feca57; font-weight: 600; }
  .src-other { color: var(--dim); }
  .scan-table th.ant-missing { color: var(--red); }
  /* Right panel: plots */
  .controls { display: flex; gap: 1rem; align-items: center; flex-wrap: wrap; margin-bottom: 1rem; }
  .controls label { font-size: 0.85rem; color: var(--dim); }
  .controls select { background: var(--bg); color: var(--text); border: 1px solid var(--border);
                     border-radius: 4px; padding: 4px 8px; font-size: 0.85rem; }
  #plot-img { max-width: 100%; border-radius: 4px; border: 1px solid var(--border); display: block; margin: 0 auto; }
  #plot-placeholder { text-align: center; color: var(--dim); padding: 3rem; }
  .footer-note { text-align: center; color: var(--dim); font-size: 0.8rem; margin-top: 1rem; }
  /* Tabs (right panel) */
  .tabs { display: flex; flex-wrap: wrap; gap: 0.4rem; margin-bottom: 0.8rem; border-bottom: 1px solid var(--border); }
  /* flex: 0 0 auto + white-space: nowrap keep every tab label fully visible: buttons
     never shrink to zero width or clip their text when another tab is selected. */
  .tab { flex: 0 0 auto; white-space: nowrap; background: transparent; color: var(--text);
         border: none; border-bottom: 2px solid transparent; padding: 0.5rem 1rem;
         font-size: 1.1rem; cursor: pointer; font-family: inherit; opacity: 0.6; }
  .tab:hover { opacity: 1; }
  .tab.active { color: var(--accent); border-bottom-color: var(--accent); font-weight: 600; opacity: 1; }
  .tab-view { height: calc(100% - 3rem); }
  #pipeline-frame { width: 100%; height: 100%; min-height: 75vh; border: 1px solid var(--border);
                    border-radius: 4px; background: #fff; }
</style>
</head>
<body>
<header>
  <h1><span>EVN Post-Processing Dashboard</span></h1>
</header>
<div class="container">
  <!-- Left panel: experiment summary -->
  <div class="panel" id="summary-panel">
    <h2>Experiment Summary</h2>
    <div id="summary-content"><p style="color:var(--dim)">Loading...</p></div>
  </div>
  <!-- Right panel: comments / standard-plots / pipeline tabs -->
  <div class="panel" id="plots-panel">
    <!-- Tab order: Comments, Standard Plots, Pipeline. All three buttons are always
         visible; the selected one is marked with the .active underline (see .tab CSS).
         Standard Plots is the default; loadPipeline() switches to Pipeline once its
         feedback page exists. -->
    <div class="tabs">
      <button class="tab" id="tab-comments" onclick="showTab('comments')">Comments</button>
      <button class="tab active" id="tab-plots" onclick="showTab('plots')">Standard Plots</button>
      <button class="tab" id="tab-pipeline" onclick="showTab('pipeline')">Pipeline</button>
    </div>
    <!-- Comments tab: general experiment note + per-station notes and status,
         persisted into the experiment toml [comments] section. -->
    <div class="tab-view" id="view-comments" style="display:none">
      <div style="margin-bottom:1rem">
        <label for="comment-general"><b>General experiment note</b> (shown in the PI letter):</label><br>
        <textarea id="comment-general" rows="3" style="width:100%"></textarea>
      </div>
      <table class="scan-table" id="comments-table" style="width:100%">
        <thead><tr><th>Station</th><th>Status</th><th style="width:70%">Note</th></tr></thead>
        <tbody></tbody>
      </table>
      <div style="margin-top:1rem">
        <button id="btn-save-comments" onclick="saveComments()">Save comments</button>
        <span id="comments-saved-msg" style="color:var(--green); display:none; margin-left:1rem">Saved.</span>
      </div>
    </div>
    <!-- Standard plots tab -->
    <div class="tab-view" id="view-plots">
      <div class="controls">
        <div><label for="sel-type">Plot type:</label><br>
          <select id="sel-type"><option value="">-- select --</option></select></div>
        <div><label for="sel-scan">Scan:</label><br>
          <select id="sel-scan"><option value="">all</option></select></div>
      </div>
      <div id="plot-area">
        <p id="plot-placeholder">Select a plot type above to view.</p>
      </div>
    </div>
    <!-- Pipeline feedback tab (only shown once the pipeline feedback page exists) -->
    <div class="tab-view" id="view-pipeline" style="display:none">
      <div class="controls" id="pipeline-controls"></div>
      <p id="pipeline-placeholder" style="display:none; text-align:center; color:var(--dim); padding:3rem">
        The pipeline feedback page is not available yet. It is generated after the EVN pipeline runs.</p>
      <iframe id="pipeline-frame" title="Pipeline feedback"></iframe>
    </div>
    <div class="footer-note">Press Ctrl+C in the terminal to stop the dashboard server.</div>
  </div>
</div>
<script>
const API = '';
let plotFiles = [];
let summaryData = null;

async function loadSummary() {
  try {
    const resp = await fetch(API + '/api/summary');
    if (!resp.ok) { throw new Error('Server returned ' + resp.status); }
    summaryData = await resp.json();
    if (summaryData.error) { throw new Error(summaryData.error); }
    const expTitle = document.getElementById('exp-title');
    if (expTitle) expTitle.textContent = summaryData.expname || '';
    renderSummary(summaryData);
  } catch (e) {
    document.getElementById('summary-content').innerHTML = '<p style="color:var(--red)">Failed to load summary: ' + e.message + '</p>';
    console.error('loadSummary error:', e);
  }
}

function renderSummary(d) {
  let h = '<div class="info-grid">';
  h += row('Experiment', '<span style="color:#ff8c00;font-weight:bold">' + d.expname + '</span>');
  h += row('Obs. date', d.obsdate + (d.timerange ? ' ' + d.timerange : ''));
  if (d.eEVNname) h += row('e-EVN run', d.eEVNname);
  (d.pi || []).forEach((p, i) => { h += row(i === 0 ? 'P.I.' : 'co-PI', `${p.name} (${p.email})`); });
  h += row('Sup. Sci.', d.supsci);
  if (d.credentials) { h += row('Username', d.credentials.username); h += row('Password', d.credentials.password); }
  if (d.feedback_page) h += row('Feedback', `<a href="${d.feedback_page}" target="_blank">${d.feedback_page}</a>`);
  if (d.archive_page) h += row('Archive', `<a href="${d.archive_page}" target="_blank">${d.archive_page}</a>`);
  h += '</div>';

  // Setup
  if (d.correlator_passes && d.correlator_passes.length) {
    h += '<div class="section"><h2>Setup</h2>';
    d.correlator_passes.forEach((cp, i) => {
      if (d.correlator_passes.length > 1) h += `<strong>Pass #${i+1}</strong><br>`;
      h += '<div class="info-grid">';
      if (cp.frequency) h += row('Frequency', cp.frequency);
      if (cp.bandwidth) h += row('Bandwidth', `${cp.subbands}-${cp.bandwidth} subbands × ${cp.channels} ch`);
      h += row('LIS file', cp.lisfile);
      h += row('MS file', cp.msfile);
      h += row('FITS-IDI files', cp.fitsidi);
      if (cp.flag_threshold !== undefined && cp.flag_threshold !== null) {
        const pct = (cp.flag_percentage !== undefined && cp.flag_percentage !== null && cp.flag_percentage >= 0)
          ? `${cp.flag_percentage.toFixed(2)}% flagged` : 'not yet applied';
        h += row('Weight flag', `threshold ${cp.flag_threshold} (${pct})`);
      }
      h += '</div>';
    });
    h += '</div>';
  }

  // Sources (editable type via dropdown)
  const typeLabels = {fringefinder:'Fringe-finder', target:'Target', calibrator:'Phase-cal', other:'Other'};
  const stAll = d.source_types || {};
  h += '<div class="section"><h2>Sources</h2>';
  h += '<table style="font-size:0.9rem;border-collapse:collapse">';
  for (const [name, stype] of Object.entries(stAll)) {
    h += `<tr><td style="padding:2px 8px"><span class="src-${stype}">${name}</span></td>`;
    h += `<td style="padding:2px 4px"><select onchange="changeSourceType('${name}',this.value)" style="background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:2px 4px;font-size:0.85rem">`;
    for (const [k,v] of Object.entries(typeLabels)) {
      h += `<option value="${k}"${k===stype?' selected':''}>${v}</option>`;
    }
    h += '</select></td></tr>';
  }
  h += '</table></div>';

  // Antennas
  const a = d.antennas || {};
  h += '<div class="section"><h2>Antennas</h2>';
  h += `<div>Observed (${(a.observed||[]).length}): ${(a.observed||[]).map(n=>`<span class="tag tag-green">${n}</span>`).join(' ')}</div>`;
  if ((a.not_observed||[]).length) h += `<div>Not observed: ${a.not_observed.map(n=>`<span class="tag tag-red">${n}</span>`).join(' ')}</div>`;
  // Reference antenna: editable via dropdown (first refant pre-selected). The remaining
  // refants stay as fallback order; changing the primary reorders the list and persists it.
  const refList = d.refant || [];
  const refPrimary = refList.length ? refList[0] : '';
  const refOptions = (a.observed || []);
  h += '<div>Ref. ant.: ';
  h += `<select onchange="changeRefant(this.value)" style="background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:2px 4px;font-size:0.85rem">`;
  if (!refOptions.length && refPrimary) h += `<option value="${refPrimary}" selected>${refPrimary}</option>`;
  refOptions.forEach(n => { h += `<option value="${n}"${n===refPrimary?' selected':''}>${n}</option>`; });
  h += '</select>';
  if (refList.length > 1) h += ' <span style="color:var(--dim);font-size:0.8rem">(fallback: ' + refList.slice(1).join(', ') + ')</span>';
  h += '</div>';
  if ((a.polswap||[]).length) h += `<div>Polswap: ${a.polswap.map(n=>`<span class="tag tag-dim">${n}</span>`).join(' ')}</div>`;
  if ((a.polconvert||[]).length) h += `<div>PolConvert: ${a.polconvert.map(n=>`<span class="tag tag-dim">${n}</span>`).join(' ')}</div>`;
  if ((a.onebit||[]).length) h += `<div>1-bit: ${a.onebit.map(n=>`<span class="tag tag-dim">${n}</span>`).join(' ')}</div>`;
  h += '</div>';

  // Scan overview table
  if (d.scans && d.scans.length && d.all_antennas) {
    const hasSnr = d.lag_snr && Object.keys(d.lag_snr).length > 0;
    h += '<div class="section"><h2>Scan Overview</h2>';
    if (hasSnr) {
      h += '<div style="font-size:0.8rem;margin-bottom:4px"><span class="tag tag-green">&#10003;</span> Observed (SNR &gt; 7) '
         + '<span class="tag" style="background:#3a3a1a;color:#f0c040">&#10003;</span> Weak (3 &lt; SNR &lt; 7) '
         + '<span class="tag tag-red">&#10003;</span> No signal (SNR &lt; 3) '
         + '<span class="tag tag-red">&#10007;</span> Scheduled but missing '
         + '<span style="color:var(--dim)">—</span> Not scheduled</div>';
    } else {
      // No lag SNR available (e.g. --no-lag): only report data presence per antenna/scan.
      h += '<div style="font-size:0.8rem;margin-bottom:4px"><span class="tag tag-green">&#10003;</span> Has data '
         + '<span class="tag tag-red">&#10007;</span> Scheduled but missing '
         + '<span style="color:var(--dim)">—</span> Not scheduled</div>';
    }
    h += '<div style="font-size:0.8rem;margin-bottom:4px">Source type: '
       + '<span class="src-fringefinder">Fringe-finder</span> · '
       + '<span class="src-target">Target</span> · '
       + '<span class="src-calibrator">Phase-cal</span></div>';
    const notObs = new Set(a.not_observed || []);
    const stMap = d.source_types || {};
    h += '<div class="scan-table-wrap"><table class="scan-table"><thead><tr><th>Scan</th><th>Source</th>';
    d.all_antennas.forEach(a => { h += notObs.has(a) ? `<th class="ant-missing">${a}</th>` : `<th>${a}</th>`; });
    h += '</tr></thead><tbody>';
    d.scans.forEach(s => {
      const stype = stMap[s.source] || 'other';
      const cls = 'src-' + stype;
      h += `<tr><td class="${cls}">${s.scanno}</td><td class="${cls}">${s.source}</td>`;
      d.all_antennas.forEach(a => {
        const st = s.antennas[a];
        if (st === 'observed') {
          const scanInt = s.scanno.replace('No','').replace(/^0+/,'') || '0';
          const snrData = d.lag_snr && d.lag_snr[scanInt] && d.lag_snr[scanInt][a];
          let maxSnr = -1;
          if (snrData) { for (const v of Object.values(snrData)) { if (v > maxSnr) maxSnr = v; } }
          if (maxSnr < 0) h += '<td class="cell-obs">&#10003;</td>';
          else if (maxSnr >= 7) h += '<td class="cell-obs" title="SNR '+maxSnr.toFixed(1)+'">&#10003;</td>';
          else if (maxSnr >= 3) h += '<td class="cell-warn" title="SNR '+maxSnr.toFixed(1)+'">&#10003;</td>';
          else h += '<td class="cell-miss" title="SNR '+maxSnr.toFixed(1)+'">&#10003;</td>';
        }
        else if (st === 'missing') h += '<td class="cell-miss">&#10007;</td>';
        else h += '<td class="cell-na">—</td>';
      });
      h += '</tr>';
    });
    h += '</tbody></table></div></div>';
  }

  document.getElementById('summary-content').innerHTML = h;
}

function row(label, value) { return `<div class="label">${label}</div><div class="value">${value || '—'}</div>`; }

async function loadPlots() {
  const resp = await fetch(API + '/api/plots');
  plotFiles = await resp.json();
  populateSelectors();
}

function populateSelectors() {
  const typeSet = new Set();
  const scanSet = new Set();
  const typeHasScans = {};  // type -> boolean: true if any file of that type contains a scan number
  plotFiles.forEach(f => {
    const m = f.match(/-(weight|auto|cross|ampphase)/);
    if (m) {
      typeSet.add(m[1]);
      const hasScan = /-scan\d+/.test(f);
      if (hasScan) typeHasScans[m[1]] = true;
      if (!(m[1] in typeHasScans)) typeHasScans[m[1]] = typeHasScans[m[1]] || false;
    }
    const sm = f.match(/-scan(\d+)/);
    if (sm) scanSet.add(sm[1]);
  });
  const selType = document.getElementById('sel-type');
  const labels = {weight:'Weight', auto:'Auto-correlation (amp/chan)', cross:'Cross-correlation (anp/chan)',
                  ampphase:'Amp+Phase vs time'};
  typeSet.forEach(t => { const o = document.createElement('option'); o.value = t; o.textContent = labels[t]||t; selType.appendChild(o); });
  const selScan = document.getElementById('sel-scan');
  [...scanSet].sort((a,b)=>+a - +b).forEach(s => { const o = document.createElement('option'); o.value = s; o.textContent = `Scan ${s}`; selScan.appendChild(o); });
  selType.addEventListener('change', onTypeChange);
  selScan.addEventListener('change', updatePlot);
  window._typeHasScans = typeHasScans;
}

function onTypeChange() {
  const ptype = document.getElementById('sel-type').value;
  const selScan = document.getElementById('sel-scan');
  if (!ptype) { selScan.disabled = false; selScan.value = ''; updatePlot(); return; }
  const hasScans = window._typeHasScans[ptype];
  if (!hasScans) {
    selScan.value = '';
    selScan.disabled = true;
  } else {
    selScan.disabled = false;
  }
  updatePlot();
}

function updatePlot() {
  const ptype = document.getElementById('sel-type').value;
  const selScan = document.getElementById('sel-scan');
  const pscan = selScan.value;
  if (!ptype) { document.getElementById('plot-area').innerHTML = '<p id="plot-placeholder">Select a plot type above to view.</p>'; return; }
  const hasScans = window._typeHasScans[ptype];
  const matches = plotFiles.filter(f => {
    if (!f.includes('-' + ptype)) return false;
    if (hasScans && pscan && !f.includes('-scan' + pscan)) return false;
    return true;
  });
  if (!matches.length) {
    document.getElementById('plot-area').innerHTML = '<p id="plot-placeholder">No plots match the current selection.</p>';
    return;
  }
  let html = '';
  matches.forEach(f => { html += `<div style="margin-bottom:1rem"><p style="font-size:0.8rem;color:var(--dim);margin-bottom:4px">${f}</p><img id="plot-img" src="/plots/${f}" alt="${f}"></div>`; });
  document.getElementById('plot-area').innerHTML = html;
}

async function changeSourceType(name, newType) {
  const resp = await fetch(API + '/api/set_source_type', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({source: name, type: newType})
  });
  const result = await resp.json();
  if (result.ok) { await loadSummary(); }
  else { alert('Error: ' + (result.error || 'unknown')); }
}

async function changeRefant(newRef) {
  const resp = await fetch(API + '/api/set_refant', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({refant: newRef})
  });
  const result = await resp.json();
  if (result.ok) { await loadSummary(); }
  else { alert('Error: ' + (result.error || 'unknown')); }
}

function showTab(name) {
  for (const tab of ['pipeline', 'plots', 'comments']) {
    document.getElementById('view-' + tab).style.display = (tab === name) ? 'block' : 'none';
    document.getElementById('tab-' + tab).classList.toggle('active', tab === name);
  }
}

/* Comments tab: statuses map to the dashboard traffic-light colours. */
const STATUS_COLORS = {success: 'var(--green)', minor: '#f0c040', major: 'var(--red)'};

function statusSelect(name, status) {
  let html = `<select class="status-select" data-station="${name}" ` +
             `style="color:${STATUS_COLORS[status]}" onchange="recolorStatus(this)">`;
  for (const s of ['success', 'minor', 'major']) {
    html += `<option value="${s}" ${s === status ? 'selected' : ''}>` +
            `${{success: '● no problem', minor: '● issues reported', major: '● could not observe'}[s]}</option>`;
  }
  return html + '</select>';
}

function recolorStatus(sel) { sel.style.color = STATUS_COLORS[sel.value]; }

async function loadComments() {
  try {
    const resp = await fetch(API + '/api/comments');
    if (!resp.ok) return;
    const data = await resp.json();
    document.getElementById('comment-general').value = data.general || '';
    const tbody = document.querySelector('#comments-table tbody');
    tbody.innerHTML = '';
    for (const [name, entry] of Object.entries(data.stations).sort()) {
      const row = document.createElement('tr');
      row.innerHTML = `<td><b>${name}</b></td><td>${statusSelect(name, entry.status)}</td>` +
        `<td><textarea class="station-note" data-station="${name}" rows="2" style="width:100%">` +
        `${entry.note || ''}</textarea></td>`;
      tbody.appendChild(row);
    }
    for (const sel of document.querySelectorAll('.status-select')) recolorStatus(sel);
  } catch (e) { console.error('loadComments error:', e); }
}

async function saveComments() {
  const stations = {};
  for (const sel of document.querySelectorAll('.status-select')) {
    stations[sel.dataset.station] = {status: sel.value, note: ''};
  }
  for (const ta of document.querySelectorAll('.station-note')) {
    if (stations[ta.dataset.station]) stations[ta.dataset.station].note = ta.value;
  }
  const resp = await fetch(API + '/api/set_comments', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({general: document.getElementById('comment-general').value, stations})
  });
  const result = await resp.json();
  if (result.ok) {
    const msg = document.getElementById('comments-saved-msg');
    msg.style.display = ''; setTimeout(() => { msg.style.display = 'none'; }, 3000);
  } else { alert('Error saving comments: ' + (result.error || 'unknown')); }
}

async function loadPipeline() {
  const frame = document.getElementById('pipeline-frame');
  const placeholder = document.getElementById('pipeline-placeholder');
  try {
    const resp = await fetch(API + '/api/pipeline');
    if (!resp.ok) return;
    const pages = await resp.json();
    if (!pages || !pages.length) {
      // No pipeline feedback yet (e.g. the pre-msops dashboard): the tab stays visible
      // but shows a note instead of an empty frame; the standard plots stay selected.
      frame.style.display = 'none';
      placeholder.style.display = '';
      return;
    }
    placeholder.style.display = 'none';
    frame.style.display = '';
    if (pages.length > 1) {
      const controls = document.getElementById('pipeline-controls');
      let sel = '<div><label for="sel-pipe">Pipeline pass:</label><br><select id="sel-pipe">';
      pages.forEach(p => { sel += `<option value="${p}">${p}</option>`; });
      sel += '</select></div>';
      controls.innerHTML = sel;
      document.getElementById('sel-pipe').addEventListener('change', e => {
        frame.src = '/pipeline/' + encodeURIComponent(e.target.value);
      });
    }
    frame.src = '/pipeline/' + encodeURIComponent(pages[0]);
    // Pipeline feedback takes precedence: show it on top by default.
    showTab('pipeline');
  } catch (e) { console.error('loadPipeline error:', e); }
}

loadSummary();
loadPlots();
loadPipeline();
loadComments();
</script>
</body>
</html>"""
    # Guard against vocabulary drift: the JS above hand-codes the station statuses
    # (STATUS_COLORS / option labels); they must match experiment_state.STATION_STATUSES.
    for status in STATION_STATUSES:
        if f"'{status}'" not in html:
            raise RuntimeError(f"Dashboard HTML is missing station status '{status}': "
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
    _DashboardHandler.dashboard_html = _build_dashboard_html()
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
