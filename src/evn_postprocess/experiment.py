#!/usr/bin/env python3
"""Defines an EVN experiment with all the relevant metadata required during the post-processing.

The metadata is obtained from different sources and/or at different stages of the post-processing.
This also keeps track of the steps that have been condducted in the post-processing so it can be
resumed, or restarted.
"""
import os
import re
import copy
import json
import subprocess
import datetime as dt
from pathlib import Path
from importlib.metadata import version as pkg_version
import tomllib
from dataclasses import dataclass, asdict
from enum import Enum
from loguru import logger
from astropy import units as u
from astropy import coordinates as coord
from rich import print as rprint
import blessed
from . import vex
from . import mstools



@dataclass
class Server:
    name: str
    user: str
    host: str
    path: Path

    def to_dict(self) -> dict:
        return {'name': self.name, 'user': self.user, 'host': self.host, 'path': str(self.path)}

    @classmethod
    def from_dict(cls, data: dict) -> 'Server':
        return cls(name=data['name'], user=data['user'], host=data['host'], path=Path(data['path']))


class Servers(list[Server]):
    """A list of Server objects with additional helper methods."""

    def names(self) -> list[str]:
        """Returns a list of all server names."""
        return [server.name for server in self]

    def __getitem__(self, key: int | str) -> Server:
        """Get a server by index (int) or by name (str)."""
        if isinstance(key, int):
            return super().__getitem__(key)
        elif isinstance(key, str):
            for server in self:
                if server.name == key:
                    return server
            raise KeyError(f"Server '{key}' not found")
        else:
            raise TypeError(f"Index must be int or str, not {type(key).__name__}")

    def to_dict(self) -> list[dict]:
        return [s.to_dict() for s in self]

    @classmethod
    def from_dict(cls, data: list[dict]) -> 'Servers':
        return cls([Server.from_dict(s) for s in data])


def retrieve_servers() -> Servers:
    """Retrieves the servers configuration from the environment.
    It will first search under the $XDG_CONFIG_HOME, $HOME/.config, or ~jops/.config directories.
    It should try to find the evnpostpro/computers.toml file. Raising an exception if it cannot be found.

    Returns:
        Servers: A list of Server objects

    Raises:
        FileNotFoundError: If the computers.toml file cannot be found.
    """
    if (configpath := (Path(os.getenv('XDG_CONFIG_HOME', Path.home())) / 'evn')).exists():
        pass
    elif (configpath := (Path(os.path.expanduser('~jops')) / '.config/evn')).exists():
        pass
    else:
        raise FileNotFoundError("No such file or directory: .config/evn/computers.toml neither "
                                "in local user nor jops")

    with open(configpath / 'computers.toml', 'rb') as f:
        servers = tomllib.load(f)

    return Servers([Server(name=s, user=servers[s]['user'], host=servers[s]['host'],
                           path=Path(servers[s]['path'])) for s in servers])


def parse_masterprojects(expname: str, server: Server) -> tuple[str, str | None]:
        """Obtains the observing epoch from the file in the server (traditionally MASTER_PROJECTS.LIS).
        In case of being an e-EVN experiment, it will add that information.

        The expected file should be a text file with one line per experiment, with expname (capital case) in the first
        column, followed by the observing epoch (YYMMDD format, or the 4-digit-year YYYYMMDD variant) in the second
        column.
        If the entry refers to an e-EVN observation (with multiple experiments in the same run), then it will have
        extra columns indicating all experiments within the run.

        Each of the extra columns will have the experiment name in the first column in a different line,
        followed again by the observing epoch.

        Args:
            expname (str): Experiment name to search for.
            server (Server): Server object with MASTER_PROJECTS.LIS location.

        Returns:
            tuple[str, str | None]:
                - The observing epoch of the experiment (YYMMDD format, or the 4-digit-year YYYYMMDD variant).
                - The e-EVN name if it is an e-EVN experiment, None otherwise.
        """
        logger.debug(f"Trying to read the experiment {expname} from {server.user}@{server.host}:{server.path}")
        process = subprocess.run(['ssh', f"{server.user}@{server.host}", f"grep {expname} {server.path}"],
                                 capture_output=True)
        if process.returncode == 1:
            raise ValueError(f"Errorcode 1 when reading {server.path} in {server.host}."
                             + f"\n{expname} was not found not in the EVN database.")
        elif process.returncode == 2:
            raise ValueError(f"Errorcode 2 when reading {server.path} in {server.host}."
                             + "\nCould not access the remote file.")
        elif process.returncode > 2:
            raise ValueError(f"Errorcode {process.returncode} when reading MASTER_PROJECTS.LIS.")

        output = [s for s in process.stdout.decode('utf-8').split('\n') if s]
        logger.debug(f"Entry in the database: {', '.join(output)}")

        if len(output) == 2:
            logger.debug(f"{expname} is an e-EVN experiment")
            # It is an e-EVN experiment!
            # One line will have EXP EPOCH.
            # The other one eEXP EPOCH EXP1 EXP2..
            entry_full, entry_exp = (0, 1) if len(output[0]) > len(output[1]) else (1, 0)
            obsdate = output[entry_exp].split()[1]
            eEVNname = output[entry_full].split()[0]
            logger.debug(f"From the e-EVN run {eEVNname} observed on {obsdate}.")
        elif len(output) == 1:
            expline = output[0].split()
            if len(expline) > 2:
                # This is an e-EVN, this experiment was the first one (so e-EVN is called the same)
                eEVNname = expline[0]
                obsdate = expline[1]
                logger.debug(f"{expname} is an e-EVN experiment (run with the same name on {obsdate})")
            else:
                eEVNname = None
                obsdate = expline[1]
                logger.debug(f"{expname} is an regular EVN experiment observed on {obsdate}")
        else:
            raise ValueError(f"{expname} not found in {server.host}:{server.path}, or server not reachable.")

        return obsdate, eEVNname


def retrieve_expname() -> str:
    """Returns the experiment name, assuming it is the name of the current directory.

    The name is validated against the EVN experiment-code shape (letters followed by
    digits, e.g. EB101, N24L2) instead of the historical MASTER_PROJECTS.LIS lookup,
    so no server access is needed.

    Raises:
        ValueError: If the directory name does not look like an experiment code.

    Returns:
        str: The experiment name
    """
    potential_experiment = Path.cwd().name
    if not re.fullmatch(r'[A-Za-z]+[0-9]+[A-Za-z0-9]*', potential_experiment):
        raise ValueError(f"The current directory name '{potential_experiment}' does not look like "
                         "an EVN experiment code. Use --expname to name the experiment explicitly.")
    return potential_experiment


def retrieve_username() -> str:
    """Returns the username of the current user.

    Returns:
        str: The username, or 'unknown' if not able to retrieve it.
    """
    return os.getenv('USER', 'unknown')


@dataclass
class Dirs:
    """Directory paths to put the different files and folders."""
    logs: Path
    # data: Path
    # results: Path
    plots: Path
    pipeline: Path
    pipe_in: Path
    pipe_out: Path
    pipe_temp: Path

    def to_dict(self) -> dict:
        return {k: str(v) for k, v in asdict(self).items()}

    @classmethod
    def from_dict(cls, data: dict) -> 'Dirs':
        return cls(**{k: Path(v) for k, v in data.items()})


@dataclass
class PI:
    name: str
    email: str

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'PI':
        return cls(**data)


@dataclass
class Credentials:
    username: str
    password: str

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'Credentials':
        return cls(**data)


@dataclass
class FlagWeight:
    """Stores the weight threshold applied (or to be applied) to the data during flagging
    and the percentage of data that have been flagged. These are the values used/obatined
    from flagweight.py at due time. Contains two properties:
    - threshold : float
        Threshold value set to flag visibilities with a weight below that value.
    - percentage : float
        Percentage of (non-zero) visibilities that were flagged.
        -1 value if not known.
    """
    threshold: float
    percentage: float

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'FlagWeight':
        return cls(**data)


class SourceType(Enum):
    target = 0
    calibrator = 1
    fringefinder = 2
    other = 3


@dataclass
class Source:
    """Defines a source by name, type (i.e. target, reference, fringefinder, other)
    and if it must be protected or not (password required to get its data).
    """
    name: str
    coordinates: coord.SkyCoord
    type: SourceType = SourceType.other
    protected: bool = False
    intent: str | None = None

    def to_dict(self) -> dict:
        return {'name': self.name, 'ra_deg': self.coordinates.ra.deg, 'dec_deg': self.coordinates.dec.deg,
                'frame': self.coordinates.frame.name, 'type': self.type.value, 'protected': self.protected,
                'intent': self.intent}

    @classmethod
    def from_dict(cls, data: dict) -> 'Source':
        return cls(name=data['name'], type=SourceType(data.get('type', 3)), protected=data.get('protected', False),
                   coordinates=coord.SkyCoord(ra=data['ra_deg']*u.deg, dec=data['dec_deg']*u.deg, frame=data.get('frame', 'icrs')),
                   intent=data.get('intent'))


class Sources(object): #list[Source]):
    def __init__(self, sources: list[Source] | None = None):
        if sources is not None:
            self._sources: list[Source] = copy.deepcopy(sources)
        else:
            self._sources = []

        self._niter : int = -1

    def append(self, new_source: Source):
        if new_source.name in self.names:
            raise KeyError(f"The source {new_source.name} is already in the list of sources.")

        self._sources.append(new_source)

    @property
    def names(self) -> list[str]:
        return [s.name for s in self._sources]

    @property
    def target(self) -> list[str]:
        return [s.name for s in self._sources if s.type == SourceType.target]

    @property
    def calibrator(self) -> list[str]:
        return [s.name for s in self._sources if s.type == SourceType.calibrator]

    @property
    def fringefinder(self) -> list[str]:
        return [s.name for s in self._sources if s.type == SourceType.fringefinder]

    @property
    def other(self) -> list[str]:
        return [s.name for s in self._sources if s.type == SourceType.other]

    def __len__(self) -> int:
        return len(self._sources)

    def __getitem__(self, key: str | int) -> Source:
        return self._sources[self.names.index(key) if isinstance(key, str) else key]

    def __delitem__(self, key: str) -> None:
        return self._sources.remove(self[key])

    def __iter__(self):
        self._niter = -1
        for src in self._sources:
            yield src

    def __next__(self) -> Source:
        if self._niter < self.__len__()-1:
            self._niter += 1
            return self._sources[self._niter]

        raise StopIteration

    def __reversed__(self) -> list[Source]:
        return self._sources[::-1]

    def __contains__(self, key: str) -> bool:
        return key in self.names

    def __str__(self) -> str:
        s = ""
        if len(self.target) > 0:
            s += f"Target: {','.join(self.target)}\n "

        if len(self.calibrator) > 0:
            s += f"Calibrator: {','.join(self.calibrator)}\n "

        if len(self.fringefinder) > 0:
            s += f"FringeFinder: {','.join(self.fringefinder)}\n "

        if len(self.other) > 0:
            s += f"Other: {','.join(self.other)}\n "

        return f"Sources([{','.join(self.names)}])\n " + s

    def to_dict(self) -> list[dict]:
        return [s.to_dict() for s in self._sources]

    @classmethod
    def from_dict(cls, data: list[dict]) -> 'Sources':
        return cls([Source.from_dict(s) for s in data])

    def calibrator_for_target(self, target: str) -> str | None:
        """Returns the associated calibrator source for a given target.
        
        It finds the calibrator source that is closest in angular separation
        to the specified target.
        
        Args:
            target: The name of the target source.
            
        Returns:
            The name of the closest calibrator source, or None if no calibrators exist.
            
        Raises:
            ValueError: If the target is not in the list of targets.
        """
        if target not in self.target:
            raise ValueError(f"Target '{target}' not found in the list of targets: {', '.join(self.target)}")
        
        if len(self.calibrator) == 0:
            return None
        elif len(self.calibrator) == 1:
            return self.calibrator[0]
        
        min_sep = None
        closest_cal = None
        for acal in self.calibrator:
            sep = self[target].coordinates.separation(self[acal].coordinates)
            if min_sep is None or sep < min_sep:
                min_sep = sep
                closest_cal = acal
        
        return closest_cal


@dataclass
class Antenna:
    name: str
    site: str = ""
    scheduled: bool = True
    observed: bool = True
    subbands: tuple = tuple()
    weights: tuple = tuple()
    polswap: bool = False
    polconvert: bool = False
    onebit: bool = False
    logfsfile: bool = False
    antabfsfile: bool = False
    opacity: bool = False  # if data have opacity correction in the ANTAB file

    def to_dict(self) -> dict:
        d = asdict(self)
        d['subbands'] = [int(x) for x in self.subbands]
        d['weights'] = [int(x) for x in self.weights]
        return d

    @classmethod
    def from_dict(cls, data: dict) -> 'Antenna':
        d = data.copy()
        d['subbands'] = tuple(d.get('subbands', []))
        d['weights'] = tuple(d.get('weights', []))
        return cls(**d)


class Antennas(list[Antenna]):
    """List of antennas (Antenna class)
    """
    @property
    def names(self) -> list[str]:
        return [a.name for a in self]

    @property
    def sites(self) -> list[str]:
        return [a.site for a in self]

    @property
    def scheduled(self) -> list[str]:
        return [a.name for a in self if a.scheduled]

    @property
    def observed(self) -> list[str]:
        return [a.name for a in self if a.observed]

    @property
    def subbands(self) -> list[tuple[int]]:
        return [a.subbands for a in self if a.observed]

    @property
    def polswap(self) -> list[str]:
        return [a.name for a in self if a.polswap]

    @property
    def polconvert(self) -> list[str]:
        return [a.name for a in self if a.polconvert]

    @property
    def onebit(self) -> list[str]:
        return [a.name for a in self if a.onebit]

    @property
    def logfsfile(self) -> list[str]:
        return [a.name for a in self if a.logfsfile]

    @property
    def antabfsfile(self) -> list[str]:
        return [a.name for a in self if a.antabfsfile]

    @property
    def opacity(self) -> list[str]:
        return [a.name for a in self if a.opacity]

    @property
    def low_weights(self) -> list[str]:
        """Antennas whose weights look unexpectedly low: less than 95% of the data sits in
        the first/last weight-histogram bin (weights <0.001 or >0.9), or nothing in the last
        bin. These are worth double-checking on the weight plots before flagging."""
        low = []
        for a in self:
            if len(a.weights) >= 7 and (total := sum(a.weights)) > 0:
                if ((a.weights[0] + a.weights[6]) / total) < 0.95 or (a.weights[6] == 0):
                    low.append(a.name)
        return low

    def __getitem__(self, key: str | int) -> Antenna:
        return super().__getitem__(self.names.index(key) if isinstance(key, str) else key)

    def __contains__(self, key: str | Antenna) -> bool:
        return key in self.names if isinstance(key, str) else super().__contains__(key)

    def __str__(self) -> str:
        s = ""
        if len(self.polswap) > 0:
            s += f"PolSwapped: {','.join(self.polswap)}\n "

        if len(self.polconvert) > 0:
            s += f"PolConverted: {','.join(self.polconvert)}\n "

        if len(self.onebit) > 0:
            s += f"1-bit data: {','.join(self.onebit)}\n "

        return f"Antennas([{','.join(self.names)}])\n Scheduled: {','.join(self.scheduled)}\n " \
               f"Observed: {','.join(self.observed)}\n " + s

    def to_dict(self) -> list[dict]:
        return [a.to_dict() for a in self]

    @classmethod
    def from_dict(cls, data: list[dict]) -> 'Antennas':
        return cls([Antenna.from_dict(a) for a in data])


@dataclass
class Scan:
    """Defines a scan in the experiment.

    ``phase_centers`` lists ALL sources correlated in the scan (multi-phase-centre
    correlations have several); ``source`` remains the primary (first) one. An empty
    tuple means "just the primary source" (single phase centre), which keeps old JSON
    state files loading unchanged.
    """
    scanno: str
    starttime: dt.datetime
    duration_s: int
    source: str
    stations_scheduled: tuple[str, ...]
    stations_observed: tuple[str, ...] = ()
    phase_centers: tuple[str, ...] = ()

    @property
    def all_sources(self) -> tuple[str, ...]:
        """All sources of the scan: the phase centres, or just the primary source."""
        return self.phase_centers if self.phase_centers else (self.source,)

    def to_dict(self) -> dict:
        return {'scanno': self.scanno, 'starttime': self.starttime.isoformat(), 'duration_s': self.duration_s,
                'source': self.source, 'stations_scheduled': list(self.stations_scheduled),
                'stations_observed': list(self.stations_observed),
                'phase_centers': list(self.phase_centers)}

    @classmethod
    def from_dict(cls, data: dict) -> 'Scan':
        return cls(scanno=data['scanno'], starttime=dt.datetime.fromisoformat(data['starttime']),
                   duration_s=data['duration_s'], source=data['source'],
                   stations_scheduled=tuple(data['stations_scheduled']),
                   stations_observed=tuple(data.get('stations_observed', [])),
                   phase_centers=tuple(data.get('phase_centers', [])))


class Scans(list[Scan]):
    """A list of scans in the experiment."""

    def to_dict(self) -> list[dict]:
        return [s.to_dict() for s in self]

    @classmethod
    def from_dict(cls, data: list[dict]) -> 'Scans':
        return cls([Scan.from_dict(s) for s in data])


@dataclass
class Subbands:
    """Defines the frequency setup of a given observation with the following data:
        - n_subbands :  int
            Number of subbands.
        - channels : int
            Number of channels per subband.
        - frequencies : array-like
            Reference frequency for each channel and subband (NxM array, with N
            number of subbands, and M number of channels per subband).
        - bandwidths : astropy.units.Quantity or float
    """
    subbands: int
    channels: int
    frequency: u.Quantity
    bandwidth: u.Quantity
    polarizations: tuple[mstools.misc.Stokes, ...]

    def to_dict(self) -> dict:
        freq_val = self.frequency.value.tolist() if hasattr(self.frequency.value, 'tolist') else self.frequency.value
        bw_val = self.bandwidth.value.tolist() if hasattr(self.bandwidth.value, 'tolist') else self.bandwidth.value
        return {'subbands': int(self.subbands), 'channels': int(self.channels), 'frequency_value': freq_val,
                'frequency_unit': str(self.frequency.unit), 'bandwidth_value': bw_val,
                'bandwidth_unit': str(self.bandwidth.unit), 'polarizations': [p.value for p in self.polarizations]}

    @classmethod
    def from_dict(cls, data: dict) -> 'Subbands':
        return cls(subbands=data['subbands'], channels=data['channels'],
                   frequency=u.Quantity(data['frequency_value'], unit=data['frequency_unit']),
                   bandwidth=u.Quantity(data['bandwidth_value'], unit=data['bandwidth_unit']),
                   polarizations=tuple(mstools.misc.Stokes(p) for p in data['polarizations']))


class CorrelatorPass:
    """Defines one correlator pass for a given experiment.
    It contains all relevant information that is pass-depended, e.g. associated .lis and
    MS files, frequency setup, etc.

    Inputs:
        lisfile : Path
            Path to the .lis file.
        msfile : Path
            Path to the .ms file.
        fitsidifile : str
            The name of the FITS IDI files associated to this correlator pass.
            Note that this is the common name for all files (without the trailing number)
        pipeline : bool
            If this pass should be pipelined.
        sources : list[Source]
            List of sources present in this correlator pass.
    """
    def __init__(self, lisfile: Path, msfile: Path, fitsidifile: str, pipeline: bool, scans: Scans | None = None,
                 sources: Sources | None = None, antennas: Antennas | None = None, flagged_weights: FlagWeight | None = None,
                 freqsetup: Subbands | None = None):
        self.lisfile = lisfile
        self.msfile = msfile
        self.fitsidifile = fitsidifile
        self.pipeline = pipeline
        self.scans: Scans = scans if scans is not None else Scans()
        self.antennas: Antennas = antennas if antennas is not None else Antennas()
        self.sources: Sources = sources if sources is not None else Sources()
        self.flagged_weights = flagged_weights
        self.freqsetup = freqsetup

    def to_dict(self) -> dict:
        from loguru import logger
        logger.debug(f"CorrelatorPass.to_dict: freqsetup={self.freqsetup}, flagged_weights={self.flagged_weights}")
        return {'lisfile': str(self.lisfile), 'msfile': str(self.msfile), 'fitsidifile': self.fitsidifile,
                'pipeline': self.pipeline, 'scans': self.scans.to_dict() if self.scans else [],
                'sources': self.sources.to_dict() if self.sources else [],
                'antennas': self.antennas.to_dict() if self.antennas else [],
                'flagged_weights': self.flagged_weights.to_dict() if self.flagged_weights else None,
                'freqsetup': self.freqsetup.to_dict() if self.freqsetup else None}

    @classmethod
    def from_dict(cls, data: dict) -> 'CorrelatorPass':
        return cls(lisfile=Path(data['lisfile']), msfile=Path(data['msfile']), fitsidifile=data['fitsidifile'],
                   pipeline=data['pipeline'], scans=Scans.from_dict(data['scans']) if data.get('scans') else None,
                   sources=Sources.from_dict(data['sources']) if data.get('sources') else None,
                   antennas=Antennas.from_dict(data['antennas']) if data.get('antennas') else None,
                   flagged_weights=FlagWeight.from_dict(data['flagged_weights']) if data.get('flagged_weights') else None,
                   freqsetup=Subbands.from_dict(data['freqsetup']) if data.get('freqsetup') else None)


def _migrate_experiment_dict(data: dict) -> dict:
    """Migrates an Experiment-shaped dict from an older schema to the current one.

    Returns the (possibly mutated) dict. The function is intentionally permissive:
    unknown keys are kept as-is so future readers can see what was stored, and
    missing keys are filled with safe defaults so an older JSON keeps loading.

    Schema history:
      - v1 (implicit): no ``_schema_version`` key. Same shape as v2 except the
        ``policy`` and ``_schema_version`` keys are absent.
      - v2: introduces ``_schema_version`` and an optional ``policy`` block.
    """
    version = int(data.get('_schema_version', 1))
    if version > Experiment.SCHEMA_VERSION:
        logger.warning(
            f"Experiment JSON has _schema_version={version} but this code only knows "
            f"up to {Experiment.SCHEMA_VERSION}. Loading optimistically; some fields "
            "may be ignored."
        )
    if version < 2:
        # v1 -> v2: add the policy slot (always None on the way up).
        data.setdefault('policy', None)
        data['_schema_version'] = 2
    return data


class Experiment:
    """Defines and EVN experiment with all relevant metadata.
    """
    def __init__(self, expname: str, obsdate: dt.date, supsci: str, dirs: Dirs, eEVNname: str | None = None,
                 steps: list | None = None, pi: list[PI] | None = None, credentials: Credentials | None = None,
                 sources: Sources | None = None, antennas: Antennas | None = None, scans: Scans | None = None,
                 refant: list[str] | None = None,
                 correlator_passes: list[CorrelatorPass] | None = None,
                 lag_pass: CorrelatorPass | None = None,
                 lag_snr: dict | None = None,
                 lag_bandpass: dict | None = None,
                 pol_diagnostics: dict | None = None,
                 no_lag: bool = False,
                 policy=None):
        self.expname = expname
        self.obsdate = obsdate
        self.supsci = supsci
        self.dirs = dirs
        self.eEVNname = eEVNname
        self.steps = steps if steps else []
        self.pi = pi if pi else []
        self.credentials = credentials
        self.sources = sources if sources else Sources()
        self.antennas = antennas if antennas else Antennas()
        self.scans = scans if scans else Scans()
        self.refant = refant if refant else []
        self.correlator_passes = correlator_passes if correlator_passes else []
        # The lag-space pass ({expname}-lag.lis / {expname}-lag.ms) is an auxiliary product
        # used ONLY to compute per-scan antenna SNR (see process.compute_lag_snr). It is kept
        # apart from `correlator_passes` on purpose: it is not a real correlator pass and must
        # never be counted as one (e.g. by `multi_phase_center`, pipeline input generation,
        # msops, tConvert, ...). Anything that processes correlator passes iterates over
        # `correlator_passes`; only the SNR computation touches `lag_pass`.
        self.lag_pass: CorrelatorPass | None = lag_pass
        self.lag_snr: dict[str, dict[str, dict[str, float]]] = lag_snr if lag_snr else {}
        # Per-scan, per-antenna fringe-peak amplitude in each IF (parallel-hand), derived from
        # the lag MS alongside lag_snr (see process.compute_lag_snr). It is the input to the
        # PolConvert reference-antenna choice: the best reference is the non-linear antenna with
        # the flattest bandpass, i.e. the smallest amplitude scatter across IFs. Shape:
        # {scan_str: {ant_name: [amp_if0, amp_if1, ...]}} (NaN where an IF has no data).
        self.lag_bandpass: dict[str, dict[str, list[float]]] = lag_bandpass if lag_bandpass else {}
        # Automatic polarization diagnostics derived from the lag MS (process.compute_lag_snr):
        # per-antenna parallel/cross-hand amplitudes for the fringe-finder scans, and the
        # resulting polswap / polconvert findings. Empty until the lag analysis runs. See
        # process.compute_lag_snr / workflow.msops for how it is produced and consumed.
        self.pol_diagnostics: dict = pol_diagnostics if pol_diagnostics else {}
        # When True, the auxiliary lag-space MS is not created and no lag SNR is computed
        # (set via the --no-lag CLI option). The per-scan antenna data check then only reports
        # whether an antenna has data in each scan, without the lag signal-to-noise comparison.
        self.no_lag: bool = no_lag
        # Temporary workaround for the broken local tConvert (set via --tConvert-in-eee /
        # --no-tConvert-in-eee). When True the tconvert step runs on eee instead of locally
        # (see process.tconvert). PolConvert is always run manually on eee (see
        # process.polconvert). Runtime-only: decided from the CLI on each run, not persisted.
        self.tconvert_in_eee: bool = True
        self._timerange: list[dt.datetime] | None = None
        # Policy is set lazily (None means "interactive defaults"). The full Policy
        # dataclass lives in evn_postprocess.policy and is loaded on demand to avoid
        # a circular import at module-load time.
        self.policy = policy

    @property
    def spectral_line(self) -> bool:
        """Returns if the experiment contains a spectral line pass."""
        return True in ['_line' in apass.lisfile.name for apass in self.correlator_passes]
    
    @property
    def multi_phase_center(self) -> bool:
        """Returns if the experiment contains a multi-phase center correlation.

        Detected either from the vex scan section (scans listing several sources) or,
        as before, from the pass layout (multiple non-spectral-line passes).
        """
        if any(len(scan.phase_centers) > 1 for scan in self.scans):
            return True
        return (len(self.correlator_passes) > 1) and (not self.spectral_line)

    @property
    def phase_center_sources(self) -> dict[str, list[str]]:
        """The phase centres per primary source, for multi-phase-centre experiments.

        Returns {primary_source: [all correlated sources of its scans]} for every
        primary source whose scans carry more than one phase centre; empty otherwise.
        This is the phase-centre -> pass mapping record: each phase centre ends up in
        its own correlator pass (own .lis/MS/FITS-IDI), whose sources are read from
        the MS as with any other multi-pass experiment.
        """
        mapping: dict[str, list[str]] = {}
        for scan in self.scans:
            if len(scan.phase_centers) > 1:
                centers = mapping.setdefault(scan.source, [])
                for center in scan.phase_centers:
                    if center not in centers:
                        centers.append(center)
        return mapping


    @property
    def timerange(self) -> list[dt.datetime] | None:
        return self._timerange

    @timerange.setter
    def timerange(self, new_time: list[dt.datetime]):
        self._timerange = new_time.copy()

    def write_log_file(self, filename: str | Path):
        """Creates the post_processing.log header with experiment info, version, and jplotter reference.

        Writes: experiment name, observation date, support scientist, evn_postprocess version,
        and convenient jplotter command snippets for manual re-runs.
        """
        try:
            ver = pkg_version('evn_postprocess')
        except Exception:
            ver = 'unknown'

        with open(filename, 'w') as logfile:
            logfile.write(f"{'#' * 60}\n")
            logfile.write("# Post-Processing log for the EVN "
                          f"experiment {self.expname}\n")
            logfile.write(f"# Observed on "
                          f"{self.obsdate.strftime('%d %b %Y') if self.obsdate else 'unknown'}.\n")
            logfile.write(f"# Support scientist: {self.supsci.capitalize()}.\n")
            logfile.write(f"# evn_postprocess version: {ver}\n")
            logfile.write(f"# Created: {dt.datetime.today().strftime('%d-%m-%Y %H:%M')}\n")
            logfile.write(f"{'#' * 60}\n\n")
            logfile.write("# Some shortcuts to run manually the standardplots in JPlotter:\n")
            logfile.write(f"ms {self.expname.lower()}.ms\nindexr\nlistr\nr\n\n")
            logfile.write("# Weight plot:\n")
            logfile.write("bl auto;fq */p;sort bl sb;pt wt;ckey sb sb[none]=1;ptsz 4;pl\n")
            logfile.write(f"save {self.expname.lower()}-weight.ps\n\n")
            logfile.write("# Amp & phase VS time plots:\n")
            logfile.write("bl Ef* -auto;fq 5/p;ch 0.1*last:0.9*last;avc vector;nxy 1 4; "
                          "pt anptime;ckey src src[none]=1;y local;ptsz 2;time none;pl\n")
            logfile.write(f"save {self.expname.lower()}-ampphase-0.ps\n")
            logfile.write("time $start to +50m;pl\n")
            logfile.write(f"save {self.expname.lower()}-ampphase-1.ps\n\n")
            logfile.write("# Auto-correlation plots:\n")
            logfile.write("scan 1;bl auto;fq */p;ch none;avt vector;avc none;pt ampfreq;ckey"
                          " p p[none]=1;sort bl;new sb false;multi true;y 0 1.6;nxy 2 4;pl\n")
            logfile.write(f"save {self.expname.lower()}-auto-0.ps\n")
            logfile.write("scan 91;pl\n")
            logfile.write(f"save {self.expname.lower()}-auto-1.ps\n\n")
            logfile.write("# Cross-correlation plots:\n")
            logfile.write("scan 1;pt anpfreq;bl Ef* -auto;fq *;ckey p['RR']=2 p['LL']=3 "
                          "p['RL']=4 p['LR']=5;nxy 2 3;y local;draw lines points;multi "
                          "true;new sb false;ptsz 4;sort bl sb;pl\n")
            logfile.write(f"save {self.expname.lower()}-cross-0.ps\n")
            logfile.write("scan 91;pl\n")
            logfile.write(f"save {self.expname.lower()}-cross-1.ps\n\n")
            logfile.write("exit\n\n")
            logfile.write(f"{'=' * 60}\n")
            logfile.write("# Commands executed during post-processing:\n")
            logfile.write(f"{'=' * 60}\n\n")

    def get_info_from_vex(self):
        """Extracts information from the VEX file."""
        if not hasattr(self, 'vixfile') or not self.vixfile.exists():
            raise FileNotFoundError(f"VEX file {getattr(self, 'vixfile', 'unknown')} not found")

        try:
            vex_data = vex.Vex(self.vixfile)
        except Exception as e:
            raise RuntimeError(f"Error parsing VEX file {self.vixfile}: {e}")
            
        if 'STATION' not in vex_data:
            raise ValueError("VEX file missing STATION section")
        if 'SOURCE' not in vex_data:
            raise ValueError("VEX file missing SOURCE section")
        if 'SCHED' not in vex_data:
            raise ValueError("VEX file missing SCHED section")
            
        try:
            for ant_code in vex_data['STATION']:
                if 'SITE' not in vex_data['STATION'][ant_code]:
                    logger.warning(f"Missing SITE info for antenna {ant_code}")
                    continue
                self.antennas.append(Antenna(name=ant_code, site=vex_data['STATION'][ant_code]['SITE']))

            for src in vex_data['SOURCE'].values():
                if 'source_name' not in src or 'ra' not in src or 'dec' not in src:
                    logger.warning(f"Incomplete source information: {src}")
                    continue
                    
                try:
                    coords_str = f"{src['ra']} {src['dec'].replace('\'', 'm').replace('"', 's')}"
                    coordinates = coord.SkyCoord(coords_str)
                except Exception as e:
                    logger.warning(f"Error parsing coordinates for source {src.get('source_name', 'unknown')}: {e}")
                    continue
                    
                self.sources.append(Source(name=src['source_name'],
                                 coordinates=coordinates,
                                 type=SourceType.other, protected=False))
            # It put a fake type and protected types... They will be overwritten when reading the jexp files

            for scanno, scan in vex_data['SCHED'].items():
                if 'start' not in scan or 'source' not in scan:
                    logger.warning(f"Incomplete scan information for {scanno}")
                    continue
                    
                try:
                    starttime = dt.datetime.strptime(scan['start'], '%Yy%jd%Hh%Mm%Ss')
                except ValueError as e:
                    logger.warning(f"Error parsing start time for scan {scanno}: {e}")
                    continue
                    
                station_entries = [s for ss, s in scan.items() if ss == 'station']
                if not station_entries:
                    logger.warning(f"No station information for scan {scanno}")
                    continue
                    
                try:
                    duration_s = max([int(s[2].replace('sec', '')) for s in station_entries])
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing duration for scan {scanno}: {e}")
                    duration_s = 0
                    
                stations_scheduled = [s[0] for s in station_entries]

                # Multi-phase-centre correlations list several 'source' entries in the
                # scan (vex $SCHED); the first one is the primary source.
                sources_in_scan = [s for ss, s in scan.items() if ss == 'source']
                if len(sources_in_scan) > 1:
                    logger.debug(f"Scan {scanno} has {len(sources_in_scan)} phase centres: "
                                 f"{', '.join(sources_in_scan)}.")

                self.scans.append(Scan(scanno, starttime=starttime,
                                       duration_s=duration_s,
                                       source=sources_in_scan[0],
                                       stations_scheduled=stations_scheduled,
                                       phase_centers=tuple(sources_in_scan)
                                       if len(sources_in_scan) > 1 else ()))
        except Exception as e:
            raise RuntimeError(f"Error processing VEX data: {e}")


    def eEVN_experiments(self) -> list[str]:
        """Returns the experiment codes (upper case) that were observed together in the
        same e-EVN session as this one, including this experiment itself.

        For a regular (non e-EVN) experiment this is simply ``[self.expname.upper()]``.
        For an e-EVN run the list is read from the ``exper_description`` field of the VEX
        file, which has the form ``e-EVN: EXP1, EXP2, ...``. If it cannot be parsed it
        falls back to just this experiment.
        """
        if self.eEVNname is None:
            return [self.expname.upper()]

        try:
            vex_data = vex.Vex(self.vixfile)
            descriptions = [block['exper_description'] for block in vex_data['EXPER'].values()
                            if 'exper_description' in block]
        except Exception as e:
            logger.warning(f"Could not read exper_description from {self.vixfile}: {e}")
            descriptions = []

        for descr in descriptions:
            if 'e-EVN' in descr:
                # Format: "e-EVN: EXP1, EXP2, ..."
                exps = [e.strip().upper() for e in descr.split(':', 1)[1].split(',') if e.strip()]
                if exps:
                    return exps

        logger.warning(f"Could not determine the e-EVN experiments for {self.expname} from {self.vixfile}.")
        return [self.expname.upper()]


    # NOTE: A previous Experiment.get_setup_from_ms() implementation was removed because
    # (a) it was dead code: it called a non-existent Antennas.add() method and would have
    # raised AttributeError at runtime, and (b) the canonical metadata loader is
    # process.get_metadata_from_ms() which is what the workflow actually uses.

    @property
    def _local_copy(self) -> Path:
        return Path(f"{self.expname.lower()}.json")


    @property
    def vixfile(self) -> Path:
        """Returns the (Path object) to the .vix file related to the experiment.
        If the file does not exist in the experiment dir (in eee), is retrieved from ccs.
        """
        return Path(f"{self.expname.upper()}.vix")


    @property
    def expsumfile(self) -> Path:
        """Returns the (Path object) to the .expsum file related to the experimet.
        If the files does not exist in the experiment dir (in eee), is retrieved from archive.
        """
        return Path(f"{self.expname.lower()}.expsum")


    @property
    def piletter(self) -> Path:
        """Returns the (Path object) to the .piletter file related to the experimet.
        If the files does not exist in the experiment dir (in eee), is retrieved from archive.
        """
        return Path(f"{self.expname.lower()}.piletter")


    @property
    def keyfile(self) -> Path:
        """Returns the (Path object) to the .key file related to the experiment.
        If the file does not exist in the experiment dir (in eee), is retrieved from vlbeer.
        """
        return Path(f"{self.expname.lower()}.key")


    @property
    def sumfile(self) -> Path:
        """Returns the (Path object) to the .sum file related to the experiment.
        If the file does not exist in the experiment dir (in eee), is retrieved from vlbeer.
        """
        return Path(f"{self.expname.lower()}.sum")


    def feedback_page(self) -> str:
        """Returns the url link to the station feedback pages for the experiment.
        """
        if self.eEVNname is not None or self.obsdate is None:
            return " -- No associated feedback pages --"

        return f"https://services.jive.eu/top/Feedback/experiment/{self.expname.upper()}"


    @property
    def archive_page(self) -> str:
        """Returns the url link to the EVN Archive pages for the experiment.
        """
        return f"https://archive.jive.eu/scripts/arch.php?exp={self.expname.upper()}"


    @staticmethod
    def exists(expname: str | None = None) -> bool:
        """Checks if there is a local copy of the Experiment object stored in a local file.
        """
        return Path(f"{expname.lower() if expname is not None else Path.cwd().name.lower()}.json").exists()


    # JSON schema version. Bump every time to_dict / from_dict change shape in
    # an incompatible way. Always store it under the "_schema_version" key. The
    # loader migrates older files in-place.
    SCHEMA_VERSION = 2

    def to_dict(self) -> dict:
        """Converts the Experiment to a plain dictionary for JSON serialization."""
        return {'_schema_version': Experiment.SCHEMA_VERSION,
                'expname': self.expname, 'obsdate': self.obsdate.isoformat() if self.obsdate else None,
                'supsci': self.supsci, 'dirs': self.dirs.to_dict() if self.dirs else None,
                'eEVNname': self.eEVNname,
                'steps': [s.to_dict() if hasattr(s, 'to_dict') else s for s in self.steps] if self.steps else [],
                'pi': [p.to_dict() for p in self.pi] if self.pi else [],
                'credentials': self.credentials.to_dict() if self.credentials else None,
                'sources': self.sources.to_dict() if self.sources else [],
                'antennas': self.antennas.to_dict() if self.antennas else [],
                'scans': self.scans.to_dict() if self.scans else [], 'refant': self.refant,
                'spectral_line': self.spectral_line,
                'policy': self.policy.to_dict() if getattr(self, 'policy', None) is not None else None,
                'correlator_passes': [cp.to_dict() for cp in self.correlator_passes] if self.correlator_passes else [],
                'lag_pass': self.lag_pass.to_dict() if self.lag_pass else None,
                'no_lag': self.no_lag,
                '_timerange': [t.isoformat() for t in self._timerange] if self._timerange else None,
                'lag_snr': self.lag_snr,
                'lag_bandpass': self.lag_bandpass,
                'pol_diagnostics': self.pol_diagnostics}

    @classmethod
    def from_dict(cls, data: dict) -> 'Experiment':
        """Creates an Experiment from a plain dictionary, migrating older schemas if needed."""
        data = _migrate_experiment_dict(data)
        exp = cls(expname=data['expname'], obsdate=dt.date.fromisoformat(data['obsdate']), supsci=data['supsci'],
                  dirs=Dirs.from_dict(data['dirs']), eEVNname=data.get('eEVNname'), steps=data.get('steps'),
                  pi=[PI.from_dict(p) for p in data['pi']] if data.get('pi') else None,
                  credentials=Credentials.from_dict(data['credentials']) if data.get('credentials') else None,
                  sources=Sources.from_dict(data['sources']) if data.get('sources') else None,
                  antennas=Antennas.from_dict(data['antennas']) if data.get('antennas') else None,
                  scans=Scans.from_dict(data['scans']) if data.get('scans') else None, refant=data.get('refant'),
                  correlator_passes=[CorrelatorPass.from_dict(cp) for cp in data['correlator_passes']]
                                     if data.get('correlator_passes') else None)
        if data.get('lag_pass'):
            exp.lag_pass = CorrelatorPass.from_dict(data['lag_pass'])
        exp.no_lag = data.get('no_lag', False)
        exp.pol_diagnostics = data.get('pol_diagnostics', {})
        if data.get('_timerange'):
            exp._timerange = [dt.datetime.fromisoformat(t) for t in data['_timerange']]
        exp.lag_snr = data.get('lag_snr', {})
        exp.lag_bandpass = data.get('lag_bandpass', {})
        # Policy is attached lazily because the Policy dataclass lives in a sibling module
        # imported only when the policy feature is actually used.
        if data.get('policy') is not None:
            from .policy import Policy  # local import avoids circular dependency
            exp.policy = Policy.from_dict(data['policy'])
        return exp

    def store(self, path: Path | None = None):
        """Atomically stores the experiment as JSON.

        Writes to ``<path>.tmp`` then renames into place so an interrupted save
        cannot leave a half-written file that subsequent loads would crash on.
        """
        target = Path(path) if path is not None else self._local_copy
        tmp = target.with_suffix(target.suffix + ".tmp")
        with open(tmp, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                # fsync isn't supported on every fs (e.g. some network mounts);
                # ignore and rely on os.replace's atomicity guarantee.
                pass
        os.replace(tmp, target)

    @staticmethod
    def load(expname: str | None = None, path: Path | None = None):
        """Loads the current Experiment that was stored in a JSON file in the indicated path.
        If path is None, it assumes the standard path of '{exp}.json' where 'exp' is the name
        of the experiment.
        """
        try:
            file_path = path if path is not None else Path(f"{expname.lower() if expname is not None \
                                                            else Path.cwd().name.lower()}.json")
            if not file_path.exists():
                raise FileNotFoundError(f"Experiment file not found: {file_path}")
                
            with open(file_path, 'r') as f:
                exp_dict = json.load(f)
                
            if not isinstance(exp_dict, dict):
                raise ValueError("Invalid experiment file format: expected dictionary")
                
            return Experiment.from_dict(exp_dict)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Could not load experiment: {e}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON in experiment file: {e}", e.doc, e.pos)
        except (KeyError, TypeError) as e:
            raise ValueError(f"Invalid experiment data structure: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error loading experiment: {e}")


    def __repr__(self, *args, **kwargs) -> str:
        rep = super().__repr__(*args, **kwargs)
        rep.replace("object", f"object ({self.expname})")
        return rep


    def __str__(self) -> str:
        return f"<Experiment {self.expname}>"


    def print(self):
        """Pretty print of the full experiment.
        """
        print('\n\n')
        rprint(f"[bold red]Experiment {self.expname.upper()}[/bold red].", sep="\n\n")
        obsdate_str = self.obsdate.strftime('%d/%m/%Y') if self.obsdate else 'Unknown'
        if self.timerange:
            rprint(f"[dim]Obs. date[/dim]: {obsdate_str} "
                   f"{'-'.join([t.time().strftime('%H:%M') for t in self.timerange])} UTC")
        else:
            rprint(f"[dim]Obs. date[/dim]: {obsdate_str}")

        if self.eEVNname is not None:
            rprint(f"[dim]e-EVN run[/dim]: {self.eEVNname}")

        for a_pi in self.pi:
            rprint(f"[dim]PI[/dim]: {a_pi.name} ({a_pi.email})")
        rprint(f"[dim]Password[/dim]: {self.credentials.password}")
        rprint(f"[dim]Sup. Sci[/dim]: {self.supsci}")
        rprint(f"[dim]Last run step[/dim]: {self.last_step}")
        print("\n")
        rprint("[bold]SETUP[/bold]")
        # loop over passes
        for i,a_pass in enumerate(self.correlator_passes):
            if len(self.correlator_passes) > 1:
                rprint(f"[bold]Correlator pass #{i}[/bold]")

            if a_pass.freqsetup is not None:
                rprint(f"Frequency: {a_pass.freqsetup.frequency.to(u.GHz):0.04}")
                rprint(f"Bandwidth: {a_pass.freqsetup.bandwidth.to(u.MHz):0.04}")
                rprint(f"{a_pass.freqsetup.subbands} x " \
                       f"{(a_pass.freqsetup.bandwidth/a_pass.freqsetup.subbands).to(u.MHz).value}-MHz subbands")
                rprint(f"{a_pass.freqsetup.channels} channels each.")
                rprint(f"lisfile: [italic]{a_pass.lisfile}[/italic]", sep="\n\n")

        print("\n")
        rprint("[bold]SOURCES[/bold]")
        for name,src_type in zip(('Fringe-finder', 'Target', 'Phase-cal'), \
                                 (SourceType.fringefinder, SourceType.target,
                                  SourceType.calibrator)):
            src = [s for s in self.sources if s.type is src_type]
            rprint(f"{name}{'' if len(src) == 1 else 's'}: [italic]" \
                   f"{', '.join([s.name for s in src])}[/italic]")

        print("\n")
        rprint("[bold]ANTENNAS[/bold]")
        ant_str = []
        for ant in self.antennas:
            if ant.observed:
                ant_str.append(ant.name)
            else:
                ant_str.append(f"[bold red]{ant.name}[/bold red]")

        rprint(f"{', '.join(ant_str)}")
        if len(self.antennas.polswap) > 0:
            rprint(f"Polswapped antennas: [italic]{', '.join(self.antennas.polswap)}[/italic]")

        if len(self.antennas.polconvert) > 0:
            rprint(f"Polconverted antennas: [italic]{', '.join(self.antennas.polconvert)}[/italic]")

        if len(self.antennas.onebit) > 0:
            rprint(f"Onebit antennas: [italic]{', '.join(self.antennas.onebit)}[/italic]")

        missing_logs = [a.name for a in self.antennas if not a.logfsfile]
        if len(missing_logs) > 0:
            rprint(f"Missing log files: [italic]{', '.join(missing_logs)}[/italic]")

        missing_antabs = [a.name for a in self.antennas if not a.antabfsfile]
        if len(missing_antabs) > 0:
            rprint(f"Missing ANTAB files: [italic]{', '.join(missing_antabs)}[/italic]")

        print("\n")


    def print_blessed(self, outputfile: str | None = None, display_in_terminal: bool = True) -> bool:
        """Pretty print of the full experiment with all available data.
        """
        term = blessed.Terminal(force_styling=True)
        s_file = []
        with term.fullscreen(), term.cbreak():
            s = term.red_on_bright_black(term.center(term.bold("EVN Post-processing of " \
                                                               f"{self.expname.upper()}")))
            s_file += [f"# EVN Post-processing of {self.expname.upper()}\n"]
            s += f"{term.normal}\n\n{term.normal}"
            obsdate_str = self.obsdate.strftime('%d/%m/%Y') if self.obsdate else 'Unknown'
            s += term.bright_black('Obs date: ') + obsdate_str
            if self.timerange is not None:
                s += f" {'-'.join([t.time().strftime('%H:%M') for t in self.timerange])} UTC\n"
                s_file += ['Obs date: ' + obsdate_str + \
                           f" {'-'.join([t.time().strftime('%H:%M') for t in self.timerange])} " \
                           "UTC\n"]
            else:
                s_file += ['Obs date: ' + obsdate_str]

            if self.eEVNname is not None:
                s += term.bright_black('From e-EVN run: ') + self.eEVNname + '\n'
                s_file += [f"From e-EVN run: {self.eEVNname}\n"]

            for i, a_pi in enumerate(self.pi):
                s += term.bright_black('P.I.: ' if i == 0 else 'co-PI:') + f"{a_pi.name.capitalize()} ({a_pi.email})\n"
                s_file += [f"{'P.I.' if i == 0 else 'co-PI'}: {a_pi.name.capitalize()} ({a_pi.email})"]

            s += term.bright_black('Sup. Sci: ') + f"{self.supsci.capitalize()}\n"
            s_file += [f"Sup. Sci: {self.supsci.capitalize()}\n"]
            s += term.bright_black('Station Feedback Link: ') + \
                 f"{term.link(self.feedback_page(), self.feedback_page())}\n"
            s_file += [f"Station Feedback Link: {self.feedback_page()}"]
            s += term.bright_black('EVN Archive Link: ') + \
                 f"{term.link(self.archive_page, self.archive_page)}\n"
            s_file += [f"EVN Archive Link: {self.archive_page}\n"]
            #s += term.bright_black('Protection Link: ') +\
                    #     term.link('https://archive.jive.eu/scripts/pipe/admin.php',
                    #       'https://archive.jive.eu/scripts/pipe/admin.php') + '\n'
            # TODO: write this too
            #s += term.bright_black('Last run step: ') + f"{self.last_step}\n\n"
            s += term.bold_green('CREDENTIALS\n')
            if self.credentials:
                s += term.bright_black('Username: ') + f"{self.credentials.username}\n"
                s += term.bright_black('Password: ') + f"{self.credentials.password}\n\n"
                s_file += ["## CREDENTIALS", f"Username: {self.credentials.username}",
                           f"Password: {self.credentials.password}\n"]
            else:
                s += "No credentials set."
                s_file += ["## CREDENTIALS", "No credentials set.\n"]

            s += term.bold_green('SETUP\n')
            s_file += ['## SETUP']

            # loop over passes
            for i,a_pass in enumerate(self.correlator_passes):
                if len(self.correlator_passes) > 1:
                    s += term.bold(f"Correlator pass #{i+1}\n")
                    s_file += [f"Correlator pass #{i+1}"]

                # If MSs are now created, it will get the info. The canonical metadata
                # loader is process.get_metadata_from_ms() (get_setup_from_ms was removed);
                # it populates every pass, so one call is enough.
                if a_pass.freqsetup is None:
                    from . import process
                    process.get_metadata_from_ms(self)
                    self.store()

                if a_pass.freqsetup is not None:
                    s += term.bright_black('Frequency: ') + \
                         f"{a_pass.freqsetup.frequency.to(u.GHz):0.04}\n"
                    s_file += [f"Frequency: {a_pass.freqsetup.frequency.to(u.GHz):0.04}"]
                    s += term.bright_black('Bandwidth: ') + \
                         f"{a_pass.freqsetup.bandwidth.to(u.MHz):0.04}.\n" + \
                         f"{a_pass.freqsetup.subbands} x " \
                         f"{(a_pass.freqsetup.bandwidth / a_pass.freqsetup.subbands).to(u.MHz).value}-MHz subbands. " \
                         f"{a_pass.freqsetup.channels} channels each.\n"
                    s_file += [f"Bandwidth: {a_pass.freqsetup.subbands} x " \
                               f"{(a_pass.freqsetup.bandwidth / a_pass.freqsetup.subbands).to(u.MHz).value}-MHz subbands. " \
                               f"{a_pass.freqsetup.channels} channels each."]
                else:
                    s += term.bright_black('Frequency:') + ' -- will get the info at MS time --\n'
                    s_file += ['Frequency:  Not retrieved yet (will happen at MS creation time)']

                s += term.bright_black('lisfile: ') + f"{a_pass.lisfile}\n"
                s += term.bright_black('MS file: ') + f"{a_pass.msfile}\n"
                s += term.bright_black('IDI files: ') + f"{a_pass.fitsidifile}\n\n"
                s_file += [f"lisfile: {a_pass.lisfile}", f"MS file: {a_pass.msfile}",
                           f"IDI files: {a_pass.fitsidifile}\n"]

            s += term.bold_green('SOURCES\n')
            s_file += ['## SOURCES']
            for name,src_type in zip(('Fringe-finder', 'Target', 'Phase-cal'), \
                                     (SourceType.fringefinder, SourceType.target,
                                      SourceType.calibrator)):
                src = [s for s in self.sources if s.type is src_type]
                key = f"{name}{'' if len(src) == 1 else 's'}: "
                s += term.bright_black(key) + \
                     f"{', '.join([s.name+term.red('*') if s.protected else s.name for s in src])}"\
                     "\n"
                s_file += [f"{key}: " \
                           f"{', '.join([s.name+'*' if s.protected else s.name for s in src])}"]

            s += term.bright_black(f"Sources with {term.red('*')} denote the " \
                                   "ones that need to be protected.\n")
            s_file += ["Sources with * denote the ones that need to be protected."]
            s += term.bold_green('ANTENNAS\n')
            s_file += ['## ANTENNAS']
            antennas_observing = [ant.name for ant in self.antennas if ant.observed]
            s += term.bright_black(f'Antennas with data ({len(antennas_observing)}):') + \
                 f"{', '.join(antennas_observing)}\n"
            s_file += [f"Antennas with data ({len(antennas_observing)}): " \
                       f"{', '.join(antennas_observing)}"]
            missing_ants = [ant.name for ant in self.antennas if not ant.observed]
            s += term.bright_black('Did not observe: ') + \
                 f"{', '.join(missing_ants) if len(missing_ants) > 0 else 'None'}\n\n"
            s_file += [f"Did not observe: " \
                       f"{', '.join(missing_ants) if len(missing_ants) > 0 else 'None'}"]
            s += term.bright_black('Reference Antenna: ') + \
                 f"{', '.join([r.capitalize() for r in self.refant])}\n"
            s_file += [f"Reference Antenna: {', '.join([r.capitalize() for r in self.refant])}"]

            if len(self.antennas.polswap) > 0:
                s += term.bright_black('Polswapped antennas: ') + \
                     f"{', '.join(self.antennas.polswap)}\n"
                s_file += [f"Polswapped antennas: {', '.join(self.antennas.polswap)}"]

            if len(self.antennas.polconvert) > 0:
                s += term.bright_black('Polconverted antennas: ') + \
                     f"{', '.join(self.antennas.polconvert)}\n"
                s_file += [f"Polconverted antennas: {', '.join(self.antennas.polconvert)}"]

            if len(self.antennas.onebit) > 0:
                s += term.bright_black('Onebit antennas: ') + f"{', '.join(self.antennas.onebit)}\n"
                s_file += [f"Onebit antennas: {', '.join(self.antennas.onebit)}"]

            # Flagged weights: threshold used and percentage of data flagged, per pass.
            for i, a_pass in enumerate(self.correlator_passes):
                fw = a_pass.flagged_weights
                if fw is None:
                    continue
                pass_lbl = f" (pass #{i+1})" if len(self.correlator_passes) > 1 else ""
                if fw.percentage is not None and fw.percentage >= 0:
                    fw_txt = (f"Flagged weights{pass_lbl}: threshold {fw.threshold}, "
                              f"{fw.percentage:.2f}% of data flagged")
                else:
                    fw_txt = f"Flagged weights{pass_lbl}: threshold {fw.threshold} (not yet applied)"
                s += term.bright_black(fw_txt) + "\n"
                s_file += [fw_txt]

            missing_logs = [a.name for a in self.antennas if (not a.logfsfile) and a.observed]
            s += term.bright_black('Missing log files: ') + \
                 f"{', '.join(missing_logs) if len(missing_logs) > 0 else 'None'}\n"
            s_file += [f"Missing log files: " \
                       f"{', '.join(missing_logs) if len(missing_logs) > 0 else 'None'}"]

            missing_antabs = [a.name for a in self.antennas if (not a.antabfsfile) and a.observed]
            s += term.bright_black('Missing ANTAB files: ') + \
                 f"{', '.join(missing_antabs) if len(missing_antabs) > 0 else 'None'}\n"
            s_file += [f"Missing ANTAB files: " \
                       f"{', '.join(missing_antabs) if len(missing_antabs) > 0 else 'None'}\n"]

            # In case of antennas not observing the full bandwidth (this may be per correlator pass)
            ss, ss_file = "", []
            try:
                if len(set([cp.freqsetup.subbands for cp in self.correlator_passes])) == 1:
                    for antenna in self.correlator_passes[0].antennas:
                        if 0 < len(antenna.subbands) < \
                               self.correlator_passes[0].freqsetup.subbands:
                            ss += f"    {antenna.name}: " \
                                  f"{' '*(3*(antenna.subbands[0]))}{antenna.subbands}\n"
                            ss_file += [f"    {antenna.name}: " \
                                        f"{' '*(3*(antenna.subbands[0]))}{antenna.subbands}"]
                else:
                    for antenna in self.correlator_passes[0].antennas:
                        for i,a_pass in enumerate(self.correlator_passes):
                            if 0 < len(antenna.subbands) < a_pass.freqsetup.subbands:
                                ss += f"    {antenna.name}: " \
                                      f"{' '*(3*(antenna.subbands[0]))}{antenna.subbands} " \
                                      f"(in correlator pass {a_pass.lisfile})\n"
                                ss_file += [f"    {antenna.name}: " \
                                            f"{' '*(3*(antenna.subbands[0]))}{antenna.subbands} " \
                                            f"(in correlator pass {a_pass.lisfile})"]

                if ss != "":
                    s += term.bright_black('Antennas with smaller bandwidth:\n')
                    s += f" Total: {list(range(self.correlator_passes[0].freqsetup.subbands))}\n"
                    s += ss
                    s_file += ['Antennas with smaller bandwidth:']
                    s_file += ss_file
            except AttributeError:
                ss += "    No freq. setup information to detect which antennas " \
                      "have a reduced bandwidth."
                ss_file += ["    No freq. setup information to detect which antennas " \
                            "have a reduced bandwidth."]

            s_final = term.wrap(s, width=term.width)
            s_file += ["\n\n## COMMENTS FROM SUP.SCI\n\n\n\n\n"]

            def print_all(ss):
                print(term.clear)
                for a_ss in ss:
                    print(a_ss)

                print(term.move_y(term.height - 3) + \
                      term.center(term.on_bright_black('press any key to continue ' \
                                                       '(or Q to cancel)')).rstrip())
                return term.inkey()#.strip()

            if (outputfile is not None) and (not Path(outputfile).exists()):
                with open(outputfile, 'w') as ofile:
                    logger.debug(f"Writing notes file {outputfile}")
                    ofile.write('\n'.join(s_file))

            if display_in_terminal:
                # Fitting the terminal
                i, i_width = 0, term.height - 5
                while i < len(s_final):
                    value = print_all(s_final[i:min(i+i_width, len(s_final)+1)])
                    if value.lower() == 'q':
                        return False
                    elif value.is_sequence and (value.name == 'KEY_UP'):
                        i = max(0, i-i_width)
                    else:
                        i += i_width

            return True
