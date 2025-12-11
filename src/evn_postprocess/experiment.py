#!/usr/bin/env python3
"""Defines an EVN experiment with all the relevant metadata required during the post-processing.

The metadata is obtained from different sources and/or at different stages of the post-processing.
This also keeps track of the steps that have been condducted in the post-processing so it can be
resumed, or restarted.
"""
import os
import glob
import copy
from tkinter import W
import numpy as np
import json
import subprocess
import datetime as dt
from typing import Optional, Union, Iterable, Any, Generator, Self
from pathlib import Path
import tomllib
from dataclasses import dataclass, asdict, fields
from collections import defaultdict
from pyrap import tables as pt
from enum import Enum
from loguru import logger
from astropy import units as u
from astropy import coordinates as coord
from rich import print as rprint
from rich import progress
import blessed
from . import vex
# from .io import parse_masterprojects  # copied function here to avoid circular importing
from . import mstools




@dataclass
class Server:
    name: str
    user: str
    host: str
    path: Path


class Servers(list[Server]):
    """A list of Server objects with additional helper methods."""

    def names(self) -> list[str]:
        """Returns a list of all server names."""
        return [server.name for server in self]

    def __getitem__(self, key: Union[int, str]) -> Server:
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


def retrieve_servers() -> Servers:
    """Retrieves the servers configuration from the environment.
    It will first search under the $XDG_CONFIG_HOME, $HOME/.config, or ~jops/.config directories.
    It should try to find the evnpostpro/computers.toml file. Raising an exception if it cannot be found.

    Returns:
        Servers: A list of Server objects

    Raises:
        FileNotFoundError: If the computers.toml file cannot be found.
    """
    if (configpath := (Path(os.getenv('XDG_CONFIG_HOME', Path.home())) / 'evn_postproc')).exists():
        pass
    elif (configpath := (Path(os.path.expanduser('~jops')) / '.config/evn_postproc')).exists():
        pass
    else:
        raise FileNotFoundError("No such file or directory: .config/evn_postproc/computers.toml neither "
                                "in local user nor jops")

    with open(configpath / 'computers.toml', 'rb') as f:
        servers = tomllib.load(f)

    return Servers([Server(name=s, user=servers[s]['user'], host=servers[s]['host'],
                           path=Path(servers[s]['path'])) for s in servers])


def parse_masterprojects(expname: str, server: Server) -> tuple[str, str | None]:
        """Obtains the observing epoch from the file in the server (traditionally MASTER_PROJECTS.LIS).
        In case of being an e-EVN experiment, it will add that information.

        The expected file should be a text file with one line per experiment, with expname (capital case) in the first
        column, followed by the observing epoch (YYMMDD format) in the second column.
        If the entry refers to an e-EVN observation (with multiple experiments in the same run), then it will have
        extra columns indicating all experiments within the run.

        Each of the extra columns will have the experiment name in the first column in a different line,
        followed again by the observing epoch.

        Args:
            expname (str): Experiment name to search for.
            server (Server): Server object with MASTER_PROJECTS.LIS location.

        Returns:
            tuple[str, str | None]:
                - The observing epoch of the experiment (YYMMDD format).
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

    Returns:
        str: The experiment name
    """
    potential_experiment = Path.cwd().name
    # It will throw an exception if the experiment is not found
    _ = parse_masterprojects(potential_experiment, retrieve_servers()['master_projects'])
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
    data: Path
    results: Path
    diagnostics: Path
    pipeline: Path
    pipe_in: Path
    pipe_out: Path
    pipe_temp: Path


@dataclass
class PI:
    name: str
    email: str


@dataclass
class Credentials:
    username: str
    password: str


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
    type: SourceType
    protected: bool


class Sources(object): #list[Source]):
    def __init__(self, sources: Optional[list[Source]] = None):
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

    def __iter__(self) -> Iterable[Source]:
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


@dataclass
class Antenna:
    name: str
    site: str = ""
    scheduled: bool = True
    observed: bool = True
    subbands: tuple = tuple()
    polswap: bool = False
    polconvert: bool = False
    onebit: bool = False
    logfsfile: bool = False
    antabfsfile: bool = False
    opacity: bool = False  # if data have opacity correction in the ANTAB file


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


@dataclass
class Scan:
    """Defines a scan in the experiment."""
    scanno: str
    starttime: dt.datetime
    duration_s: int
    source: str
    stations_scheduled: tuple[str]
    stations_observed: tuple[str] = ()


class Scans(list[Scan]):
    """A list of scans in the experiment."""
    pass


class ExperimentJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for Experiment objects."""
    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return {'__type__': 'datetime', 'value': obj.isoformat()}
        elif isinstance(obj, dt.date):
            return {'__type__': 'date', 'value': obj.isoformat()}
        elif isinstance(obj, Path):
            return {'__type__': 'Path', 'value': str(obj)}
        elif isinstance(obj, u.Quantity):
            return {'__type__': 'Quantity', 'value': obj.value.tolist() if hasattr(obj.value, 'tolist') else obj.value, 'unit': str(obj.unit)}
        elif isinstance(obj, coord.SkyCoord):
            return {'__type__': 'SkyCoord', 'ra': obj.ra.deg, 'dec': obj.dec.deg, 'frame': obj.frame.name}
        elif isinstance(obj, SourceType):
            return {'__type__': 'SourceType', 'value': obj.value}
        elif isinstance(obj, mstools.misc.Stokes):
            return {'__type__': 'Stokes', 'value': obj.value}
        elif isinstance(obj, np.ndarray):
            return {'__type__': 'ndarray', 'value': obj.tolist(), 'dtype': str(obj.dtype)}
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, (Server, PI, Credentials, FlagWeight, Source, Antenna, Scan, Subbands)):
            return {'__type__': obj.__class__.__name__, 'data': asdict(obj)}
        elif hasattr(obj, '__dataclass_fields__'):
            return {'__type__': obj.__class__.__name__, 'data': asdict(obj)}
        elif isinstance(obj, Servers):
            return {'__type__': 'Servers', 'data': [asdict(s) for s in obj]}
        elif isinstance(obj, Sources):
            return {'__type__': 'Sources', 'data': [asdict(s) for s in obj._sources]}
        elif isinstance(obj, Antennas):
            return {'__type__': 'Antennas', 'data': [asdict(a) for a in obj]}
        elif isinstance(obj, Scans):
            return {'__type__': 'Scans', 'data': [asdict(s) for s in obj]}
        elif isinstance(obj, CorrelatorPass):
            return {'__type__': 'CorrelatorPass', 'data': {
                'lisfile': obj.lisfile,
                'msfile': obj.msfile,
                'fitsidifile': obj.fitsidifile,
                'pipeline': obj.pipeline,
                'scans': obj.scans,
                'antennas': obj.antennas,
                'flagged_weights': obj.flagged_weights,
                'freqsetup': obj.freqsetup
            }}
        elif isinstance(obj, Dirs):
            return {'__type__': 'Dirs', 'data': asdict(obj)}
        return super().default(obj)


def experiment_json_decoder(dct):
    """Custom JSON decoder for Experiment objects."""
    if '__type__' in dct:
        obj_type = dct['__type__']
        if obj_type == 'datetime':
            return dt.datetime.fromisoformat(dct['value'])
        elif obj_type == 'date':
            return dt.date.fromisoformat(dct['value'])
        elif obj_type == 'Path':
            return Path(dct['value'])
        elif obj_type == 'Quantity':
            return u.Quantity(dct['value'], unit=dct['unit'])
        elif obj_type == 'SkyCoord':
            return coord.SkyCoord(ra=dct['ra']*u.deg, dec=dct['dec']*u.deg, frame=dct['frame'])
        elif obj_type == 'SourceType':
            return SourceType(dct['value'])
        elif obj_type == 'Stokes':
            return mstools.misc.Stokes(dct['value'])
        elif obj_type == 'ndarray':
            return np.array(dct['value'], dtype=dct['dtype'])
        elif obj_type == 'Server':
            data = dct['data']
            data['path'] = Path(data['path']) if isinstance(data['path'], str) else data['path']
            return Server(**data)
        elif obj_type == 'PI':
            return PI(**dct['data'])
        elif obj_type == 'Credentials':
            return Credentials(**dct['data'])
        elif obj_type == 'FlagWeight':
            return FlagWeight(**dct['data'])
        elif obj_type == 'Source':
            data = dct['data']
            return Source(
                name=data['name'],
                coordinates=data['coordinates'],
                type=data['type'],
                protected=data['protected']
            )
        elif obj_type == 'Antenna':
            return Antenna(**dct['data'])
        elif obj_type == 'Scan':
            return Scan(**dct['data'])
        elif obj_type == 'Subbands':
            return Subbands(**dct['data'])
        elif obj_type == 'Task':
            from . import workflow
            return workflow.Task(**dct['data'])
        elif obj_type == 'Servers':
            servers = []
            for s_data in dct['data']:
                s_data['path'] = Path(s_data['path']) if isinstance(s_data['path'], str) else s_data['path']
                servers.append(Server(**s_data))
            return Servers(servers)
        elif obj_type == 'Sources':
            sources = []
            for s_data in dct['data']:
                sources.append(Source(
                    name=s_data['name'],
                    coordinates=s_data['coordinates'],
                    type=s_data['type'],
                    protected=s_data['protected']
                ))
            return Sources(sources)
        elif obj_type == 'Antennas':
            return Antennas([Antenna(**a_data) for a_data in dct['data']])
        elif obj_type == 'Scans':
            return Scans([Scan(**s_data) for s_data in dct['data']])
        elif obj_type == 'CorrelatorPass':
            data = dct['data']
            return CorrelatorPass(
                lisfile=data['lisfile'],
                msfile=data['msfile'],
                fitsidifile=data['fitsidifile'],
                pipeline=data['pipeline'],
                scans=data.get('scans'),
                antennas=data.get('antennas'),
                flagged_weights=data.get('flagged_weights'),
                freqsetup=data.get('freqsetup')
            )
        elif obj_type == 'Dirs':
            data = dct['data']
            for key in data:
                if isinstance(data[key], str):
                    data[key] = Path(data[key])
            return Dirs(**data)
    return dct


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
    polarizations: tuple[mstools.misc.Stokes]


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
        self.flagged_weights = flagged_weights
        self.freqsetup = freqsetup


class Experiment:
    """Defines and EVN experiment with all relevant metadata.
    """
    def __init__(self, expname: str, obsdate: dt.date, supsci: str, dirs: Dirs, eEVNname: str | None = None,
                 steps: list | None = None, pi: list[PI] | None = None, credentials: Credentials | None = None,
                 sources: Sources | None = None, antennas: Optional[Antennas] = None, scans: Optional[Scans] = None,
                 refant: Optional[list[str]] = None, spectral_line: bool = False,
                 correlator_passes: Optional[list[CorrelatorPass]] = None):
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
        self.spectral_line = spectral_line
        self.correlator_passes = correlator_passes if correlator_passes else []
        self._log_file: Path = self.dirs.logs / 'processing.log'
        self._timerange: list[dt.datetime] | None = None

    @property
    def timerange(self) -> list[dt.datetime] | None:
        return self._timerange

    @timerange.setter
    def timerange(self, new_time: list[dt.datetime]):
        self._timerange = new_time.copy()

    def write_log_file(self):
        # Writes down some snippets for jplotter in case the standard one fails.
        with open(self._log_file, 'a') as logfile:
            logfile.write("This is the log file for the Post-Processing of the EVN " \
                            f"experiment {self.expname}, observed on "\
                            f"{self.obsdate.strftime('%d %b %Y') if self.obsdate else 'unknown'}.\n")
            logfile.write("The associated JIVE support scientist is " \
                            f"{self.supsci.capitalize()}.\n\n")
            logfile.write("# Some shortcuts to run manually the standardplots in JPlotter:\n")
            logfile.write(f"ms {self.expname.lower()}.ms\nindexr\nlistr\nr\n\n")
            logfile.write("# Weight plot:\n")
            logfile.write("bl auto;fq */p;sort bl sb;pt wt;ckey sb sb[none]=1;ptsz 4;pl\n")
            logfile.write(f"save {self.expname.lower()}-weight.ps\n\n")
            logfile.write("# Amp & phase VS time plots:\n")
            logfile.write("bl Ef* -auto;fq 5/p;ch 0.1*last:0.9*last;avc vector;nxy 1 4; " \
                            "pt anptime;ckey src src[none]=1;y local;ptsz 2;time none;pl\n")
            logfile.write(f"save {self.expname.lower()}-ampphase-0.ps\n")
            logfile.write("time $start to +50m;pl\n")
            logfile.write(f"save {self.expname.lower()}-ampphase-1.ps\n\n")
            logfile.write("# Auto-correlation plots:\n")
            logfile.write("scan 1;bl auto;fq */p;ch none;avt vector;avc none;pt ampfreq;ckey" \
                            " p p[none]=1;sort bl;new sb false;multi true;y 0 1.6;nxy 2 4;pl\n")
            logfile.write(f"save {self.expname.lower()}-auto-0.ps\n")
            logfile.write("scan 91;pl\n")
            logfile.write(f"save {self.expname.lower()}-auto-1.ps\n\n")
            logfile.write("# Cross-correlation plots:\n")
            logfile.write("scan 1;pt anpfreq;bl Ef* -auto;fq *;ckey p['RR']=2 p['LL']=3 " \
                            "p['RL']=4 p['LR']=5;nxy 2 3;y local;draw lines points;multi " \
                            "true;new sb false;ptsz 4;sort bl sb;pl\n")
            logfile.write(f"save {self.expname.lower()}-cross-0.ps\n")
            logfile.write("scan 91;pl\n")
            logfile.write(f"save {self.expname.lower()}-cross-1.ps\n\n")
            logfile.write("exit\n")

    def get_info_from_vex(self):
        """Extracts information from the VEX file."""
        if not self.vixfile.exists():
            raise FileNotFoundError(f"VEX file {self.vixfile} not found")

        vex_data = vex.Vex(self.vixfile)
        for ant_code in vex_data['STATION']:
            self.antennas.append(Antenna(name=ant_code, site=vex_data['STATION'][ant_code]['SITE']))

        for src in vex_data['SOURCE'].values():
            self.sources.append(Source(name=src['source_name'],
                             coordinates=coord.SkyCoord(f"{src['ra']} {src['dec'].replace('\'', 'm').replace('\"', 's')}"),
                             type=SourceType.other, protected=False))
            # It put a fake type and protected types... They will be overwritten when reading the jexp files

        for scanno, scan in vex_data['SCHED'].items():
            self.scans.append(Scan(scanno, starttime=dt.datetime.strptime(scan['start'], '%Yy%jd%Hh%Mm%Ss'),
                                   duration_s=max([int(s[2].replace('sec', '')) for ss, s in scan.items() if ss == 'station']),
                                   source=scan['source'], stations_scheduled=[s[0] for ss, s in scan.items() if ss == 'station']))


    def get_setup_from_ms(self):
        """Obtains the time range, antennas, sources, and frequencies of the observation
        from all existing passes with MS files and incorporate them into the current object.
        """
        for i,a_pass in enumerate(self.correlator_passes):
            if (i > 0) and ('_line' not in ''.join(glob.glob(f"{self.expname.lower()}*.lis"))):
                # then this is just a multiphase center with all setups identical. Do not loop
                # through all MSs.
                a_pass.antennas = self.correlator_passes[0].antennas
                a_pass.sources = self.correlator_passes[0].sources
                a_pass.freqsetup = self.correlator_passes[0].freqsetup
                continue

            a_pass.antennas = Antennas()
            try:
                with pt.table(a_pass.msfile.name, readonly=True, ack=False) as ms:
                    with pt.table(ms.getkeyword('ANTENNA'), readonly=True, ack=False) as ms_ant:
                        antenna_col = ms_ant.getcol('NAME')
                        for ant_name in antenna_col:
                            ant = Antenna(name=ant_name, observed=True)
                            a_pass.antennas.add(ant)

                            if ant_name.capitalize() in self.antennas.names:
                                self.antennas[ant_name.capitalize()].observed = True
                            else:
                                ant = Antenna(name=ant_name, observed=True)
                                self.antennas.add(ant)

                    with pt.table(ms.getkeyword('DATA_DESCRIPTION'),
                                  readonly=True, ack=False) as ms_spws:
                        spw_names = ms_spws.getcol('SPECTRAL_WINDOW_ID')

                    ant_subband = defaultdict(set)
                    print('\nReading the MS to find the antennas that actually observed...')
                    with progress.Progress() as progress_bar:
                        task = progress_bar.add_task("[yellow]Reading MS...", total=len(ms))
                        for (start, nrow) in mstools.misc.chunkert(0, len(ms), 100):
                            ants1 = ms.getcol('ANTENNA1', startrow=start, nrow=nrow)
                            ants2 = ms.getcol('ANTENNA2', startrow=start, nrow=nrow)
                            spws = ms.getcol('DATA_DESC_ID', startrow=start, nrow=nrow)
                            msdata = ms.getcol('DATA', startrow=start, nrow=nrow)

                            for ant_i,antenna_name in enumerate(antenna_col):
                                for spw in spw_names:
                                    cond = np.where((ants1 == ant_i) & (ants2 == ant_i) \
                                                    & (spws == spw))
                                    if not (abs(msdata[cond]) < 1e-5).all():
                                        ant_subband[antenna_name].add(spw)

                            progress_bar.update(task, advance=nrow)

                    for antenna_name in self.antennas.names:
                        if antenna_name in a_pass.antennas:
                            a_pass.antennas[antenna_name].subbands = \
                                      tuple(ant_subband[antenna_name])
                            a_pass.antennas[antenna_name].observed = \
                                      len(a_pass.antennas[antenna_name].subbands) > 0

                    # Takes the predefined "best" antennas as reference
                    if len(self.refant) == 0:
                        for ant in ('Ef', 'O8', 'Ys', 'Mc', 'Gb', 'At', 'Pt'):
                            if (ant in a_pass.antennas) and (a_pass.antennas[ant].observed):
                                self.refant = [ant, ]
                                break

                    with pt.table(ms.getkeyword('FIELD'), readonly=True, ack=False) as ms_field:
                        a_pass.sources = ms_field.getcol('NAME')

                    with pt.table(ms.getkeyword('OBSERVATION'), readonly=True, ack=False) as ms_obs:
                        self.timerange = dt.datetime(1858, 11, 17, 0, 0, 2) + \
                             ms_obs.getcol('TIME_RANGE')[0]*dt.timedelta(seconds=1)
                    with pt.table(ms.getkeyword('SPECTRAL_WINDOW'),
                                  readonly=True, ack=False) as ms_spw:
                        a_pass.freqsetup = Subbands(ms_spw.getcol('NUM_CHAN')[0],
                                                    ms_spw.getcol('CHAN_FREQ'),
                                                    ms_spw.getcol('TOTAL_BANDWIDTH')[0])
            except RuntimeError:
                print(f"WARNING: {a_pass.msfile} not found.")

        for antenna_name in self.antennas.names:
            try:
                self.antennas[antenna_name].observed = any([cp.antennas[antenna_name].observed \
                                                            for cp in self.correlator_passes])
            except ValueError:
                print(f"Antenna {antenna_name} in list not present in the MS.")


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


    def store(self, path: Optional[Path] = None):
        """Stores the current Experiment into a JSON file in the indicated path. If not provided,
        it will be '{expname.lower()}.json' where exp is the name of the experiment.
        """
        exp_dict = {
            'expname': self.expname,
            'obsdate': self.obsdate,
            'supsci': self.supsci,
            'dirs': self.dirs,
            'eEVNname': self.eEVNname,
            'steps': self.steps,
            'pi': self.pi,
            'credentials': self.credentials,
            'sources': self.sources,
            'antennas': self.antennas,
            'scans': self.scans,
            'refant': self.refant,
            'spectral_line': self.spectral_line,
            'correlator_passes': self.correlator_passes,
            '_timerange': self._timerange
        }
        with open(path if path is not None else self._local_copy, 'w') as f:
            json.dump(exp_dict, f, cls=ExperimentJSONEncoder, indent=2)


    @staticmethod
    def load(expname: str | None = None, path: Optional[Path] = None):
        """Loads the current Experiment that was stored in a JSON file in the indicated path.
        If path is None, it assumes the standard path of '{exp}.json' where 'exp' is the name
        of the experiment.
        """
        with open(path if path is not None else Path(f"{expname.lower() if expname is not None \
                                                        else Path.cwd().name.lower()}.json"), 'r') as f:
            exp_dict = json.load(f, object_hook=experiment_json_decoder)

        exp = Experiment(
            expname=exp_dict['expname'],
            obsdate=exp_dict['obsdate'],
            supsci=exp_dict['supsci'],
            dirs=exp_dict['dirs'],
            eEVNname=exp_dict.get('eEVNname'),
            steps=exp_dict.get('steps'),
            pi=exp_dict.get('pi'),
            credentials=exp_dict.get('credentials'),
            sources=exp_dict.get('sources'),
            antennas=exp_dict.get('antennas'),
            scans=exp_dict.get('scans'),
            refant=exp_dict.get('refant'),
            spectral_line=exp_dict.get('spectral_line', False),
            correlator_passes=exp_dict.get('correlator_passes')
        )
        exp._timerange = exp_dict.get('_timerange')
        return exp


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
        if self.timerange:
            rprint(f"[dim]Obs. date[/dim]: {self.obsdate.strftime('%d/%m/%Y')} "
                   f"{'-'.join([t.time().strftime('%H:%M') for t in self.timerange])} UTC")
        else:
            rprint(f"[dim]Obs. date[/dim]: {self.obsdate.strftime('%d/%m/%Y')}")

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
            s += term.bright_black('Obs date: ') + self.obsdate.strftime('%d/%m/%Y')
            if self.timerange is not None:
                s += f" {'-'.join([t.time().strftime('%H:%M') for t in self.timerange])} UTC\n"
                s_file += ['Obs date: ' + self.obsdate.strftime('%d/%m/%Y') + \
                           f" {'-'.join([t.time().strftime('%H:%M') for t in self.timerange])} " \
                           "UTC\n"]
            else:
                s_file += ['Obs date: ' + self.obsdate.strftime('%d/%m/%Y')]

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

                # If MSs are now created, it will get the info.
                if a_pass.freqsetup is None:
                    self.get_setup_from_ms()
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
                if len(set([cp.freqsetup.n_subbands for cp in self.correlator_passes])) == 1:
                    for antenna in self.correlator_passes[0].antennas:
                        if 0 < len(antenna.subbands) < \
                               self.correlator_passes[0].freqsetup.n_subbands:
                            ss += f"    {antenna.name}: " \
                                  f"{' '*(3*(antenna.subbands[0]))}{antenna.subbands}\n"
                            ss_file += [f"    {antenna.name}: " \
                                        f"{' '*(3*(antenna.subbands[0]))}{antenna.subbands}"]
                else:
                    for antenna in self.correlator_passes[0].antennas:
                        for i,a_pass in enumerate(self.correlator_passes):
                            if 0 < len(antenna.subbands) < a_pass.freqsetup.n_subbands:
                                ss += f"    {antenna.name}: " \
                                      f"{' '*(3*(antenna.subbands[0]))}{antenna.subbands} " \
                                      f"(in correlator pass {a_pass.lisfile})\n"
                                ss_file += [f"    {antenna.name}: " \
                                            f"{' '*(3*(antenna.subbands[0]))}{antenna.subbands} " \
                                            f"(in correlator pass {a_pass.lisfile})"]

                if ss != "":
                    s += term.bright_black('Antennas with smaller bandwidth:\n')
                    s += f" Total: {list(range(self.correlator_passes[0].freqsetup.n_subbands))}\n"
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
                    print('writing file', s_file)
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
