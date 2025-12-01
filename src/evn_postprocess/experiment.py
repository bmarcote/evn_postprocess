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
import pickle
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
from astropy import units as u
from astropy import coordinates as coord
from rich import print as rprint
from rich import progress
import blessed
from . import dialog
from . import vex
from . import io 
from . import mstools 
from .workflow import Task




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
    # TODO: jump from home dir, jops home dir...
    with open(Path(os.getenv('XDG_CONFIG_HOME', Path.home() / '.config')) / 'evn' / 'computers.toml', 'rb') as f:
        servers = tomllib.load(f)
    
    return Servers([Server(name=s, user=servers[s]['user'], host=servers[s]['host'], path=Path(servers[s]['path'])) for s in servers])


def retrieve_expname() -> str:
    """Returns the experiment name, assuming it is the name of the current directory.
    
    Returns:
        str: The experiment name
    """
    potential_experiment = Path.cwd().name
    # It will throw an exception if the experiment is not found
    _ = io.parse_masterprojects(potential_experiment, retrieve_servers()['master_projects'])
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
    weights: np.ndarray = np.array([])


class Antennas(object):
    """List of antennas (Antenna class)
    """
    def __init__(self, antennas: Optional[list[Antenna]] = None):
        if antennas is not None:
            self._antennas: list[Antenna] = copy.deepcopy(antennas)
        else:
            self._antennas = []

        self._niter : int = -1

    def add(self, new_antenna: Antenna):
        if new_antenna.name in self.names:
            raise KeyError(f"The antenna {new_antenna.name} is already in the list of antennas.")

        self._antennas.append(new_antenna)

    @property
    def names(self) -> list[str]:
        return [a.name for a in self._antennas]

    @property
    def sites(self) -> list[str]:
        return [a.site for a in self._antennas]

    @property
    def scheduled(self) -> list[str]:
        return [a.name for a in self._antennas if a.scheduled]

    @property
    def observed(self) -> list[str]:
        return [a.name for a in self._antennas if a.observed]

    @property
    def subbands(self) -> list[tuple[int]]:
        return [a.subbands for a in self._antennas if a.observed]

    @property
    def polswap(self) -> list[str]:
        return [a.name for a in self._antennas if a.polswap]

    @property
    def polconvert(self) -> list[str]:
        return [a.name for a in self._antennas if a.polconvert]

    @property
    def onebit(self) -> list[str]:
        return [a.name for a in self._antennas if a.onebit]

    @property
    def logfsfile(self) -> list[str]:
        return [a.name for a in self._antennas if a.logfsfile]

    @property
    def antabfsfile(self) -> list[str]:
        return [a.name for a in self._antennas if a.antabfsfile]

    @property
    def opacity(self) -> list[str]:
        return [a.name for a in self._antennas if a.opacity]

    def __len__(self) -> int:
        return len(self._antennas)

    def __getitem__(self, key: str) -> Antenna:
        return self._antennas[self.names.index(key)]

    def __delitem__(self, key: str) -> None:
        return self._antennas.remove(self[key])

    def __iter__(self) -> Iterable[Antenna]:
        self._niter = -1
        for ant in self._antennas:
            yield ant

    def __next__(self) -> Antenna:
        if self._niter < self.__len__()-1:
            self._niter += 1
            return self._antennas[self._niter]

        raise StopIteration

    def __reversed__(self) -> list[Antenna]:
        return self._antennas[::-1]

    def __contains__(self, key: str) -> bool:
        return key in self.names

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
    stations_scheduled: list[str]
    stations_observed: list[str] = []


class Scans(list[Scan]):
    """A list of scans in the experiment."""
    pass


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


@dataclass
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
    lisfile: Path
    msfile: Path
    fitsidifile: str
    pipeline: bool
    scans: Scans = Scans()
    sources: Sources = Sources()
    antennas: Antennas = Antennas()
    flagged_weights: Optional[FlagWeight] = None
    freqsetup: Optional[Subbands] = None



@dataclass
class Experiment:
    """Defines and EVN experiment with all relevant metadata.
    """
    expname: str
    obsdate: dt.date
    supsci: str
    dirs: Dirs
    eEVNname: Optional[str] = None
    steps: list[Task] = []
    pi: list[PI] = []
    credentials: Credentials | None = None
    sources: Sources = Sources()
    antennas: Antennas = Antennas()
    scans: Scans = Scans()
    refant: list[str] = []
    spectral_line: bool = False   # Meaning there is a spectral line correlation (not just continuum)
    correlator_passes: list[CorrelatorPass] = []
    _log_file: Path = Path("logs/processing.log")

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
                                   duration_s=max([int(s[2].replace('sec', '')) for s in scan['station']]),
                                   source=scan['source'], stations_scheduled=[s[0] for s in scan['station']]))


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
        """Stores the current Experiment into a file in the indicated path. If not provided,
        it will be '.{expname.lower()}.obj' where exp is the name of the experiment.
        """
        with open(path if path is not None else self._local_copy, 'wb') as f:
            pickle.dump(self, f)


    @staticmethod
    def load(expname: str | None = None, path: Optional[Path] = None):
        """Loads the current Experiment that was stored in a file in the indicated path.
        If path is None, it assumes the standard path of '.{exp}.json' where 'exp' is the name
        of the experiment.
        """
        with open(path if path is not None else Path(f"{expname.lower() if expname is not None \
                                                        else Path.cwd().name.lower()}.json"), 'rb') as f:
            obj = pickle.load(f)
            # obj = json.load(f, cls=ExpJsonEncoder)

        return obj


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
        if None not in self.timerange:
            rprint(f"[dim]Obs. date[/dim]: {self.obsdatetime.strftime('%d/%m/%Y')} "
                   f"{'-'.join([t.time().strftime('%H:%M') for t in self.timerange])} UTC")
        else:
            rprint(f"[dim]Obs. date[/dim]: {self.obsdatetime.strftime('%d/%m/%Y')}")

        if self.eEVNname is not None:
            rprint(f"[dim]e-EVN run[/dim]: {self.eEVNname}")

        rprint(f"[dim]PI[/dim]: {self.piname} ({self.email})")
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
                rprint(f"Frequency: {a_pass.freqsetup.frequencies[0,0]/1e9:0.04}-" \
                       f"{a_pass.freqsetup.frequencies[-1,-1]/1e9:0.04} GHz")
                rprint(f"{a_pass.freqsetup.n_subbands} x " \
                       f"{a_pass.freqsetup.bandwidths.to(u.MHz).value}-MHz subbands")
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
            s += term.bright_black('Obs date: ') + self.obsdatetime.strftime('%d/%m/%Y')
            if None not in self.timerange:
                s += f" {'-'.join([t.time().strftime('%H:%M') for t in self.timerange])} UTC\n"
                s_file += ['Obs date: ' + self.obsdatetime.strftime('%d/%m/%Y') + \
                           f" {'-'.join([t.time().strftime('%H:%M') for t in self.timerange])} " \
                           "UTC\n"]
            else:
                s_file += ['Obs date: ' + self.obsdatetime.strftime('%d/%m/%Y')]

            if self.eEVNname is not None:
                s += term.bright_black('From e-EVN run: ') + self.eEVNname + '\n'
                s_file += [f"From e-EVN run: {self.eEVNname}\n"]

            if isinstance(self.piname, list):
                for a_piname,an_email,n in zip(self.piname, self.email,
                                               ('', *['co-']*(len(self.piname)-1))):
                    s += term.bright_black(n+'P.I.: ') + f"{a_piname.capitalize()} ({an_email})\n"
                    s_file += [f"P.I.: {a_piname.capitalize()} ({an_email})"]
            else:
                s += term.bright_black('P.I.: ') + f"{self.piname.capitalize()} ({self.email})\n"
                s_file += [f"P.I.: {self.piname.capitalize()} ({self.email})"]

            s += term.bright_black('Sup. Sci: ') + f"{self.supsci.capitalize()}\n"
            s_file += [f"Sup. Sci: {self.supsci.capitalize()}\n"]
            s += term.bright_black('Station Feedback Link: ') + \
                 f"{term.link(self.feedback_page(), self.feedback_page())}\n"
            s_file += [f"Station Feedback Link: {self.feedback_page()}"]
            s += term.bright_black('EVN Archive Link: ') + \
                 f"{term.link(self.archive_page, self.archive_page)}\n"
            s_file += [f"EVN Archive Link: {self.archive_page}\n"]
            s += term.bright_black('Protection Link: ') +\
                 term.link('https://archive.jive.eu/scripts/pipe/admin.php',
                           'https://archive.jive.eu/scripts/pipe/admin.php') + '\n'
            s += term.bright_black('Last run step: ') + f"{self.last_step}\n\n"
            s += term.bold_green('CREDENTIALS\n')
            s += term.bright_black('Username: ') + f"{self.credentials.username}\n"
            s += term.bright_black('Password: ') + f"{self.credentials.password}\n\n"
            s_file += ["## CREDENTIALS", f"Username: {self.credentials.username}",
                       f"Password: {self.credentials.password}\n"]

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
                         f"{a_pass.freqsetup.frequencies[0,0]/1e9:0.04}-" \
                         f"{a_pass.freqsetup.frequencies[-1,-1]/1e9:0.04} GHz.\n"
                    s_file += [f"Frequency: {a_pass.freqsetup.frequencies[0,0]/1e9:0.04}-" \
                               f"{a_pass.freqsetup.frequencies[-1,-1]/1e9:0.04} GHz."]
                    s += term.bright_black('Bandwidth: ') + \
                         f"{a_pass.freqsetup.n_subbands} x " \
                         f"{a_pass.freqsetup.bandwidths.to(u.MHz).value}-MHz subbands. " \
                         f"{a_pass.freqsetup.channels} channels each.\n"
                    s_file += [f"Bandwidth: {a_pass.freqsetup.n_subbands} x " \
                               f"{a_pass.freqsetup.bandwidths.to(u.MHz).value}-MHz subbands. " \
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
            s += term.bright_black('Sources to standardplot: ') + \
                 f"{', '.join(self.sources_stdplot)}\n\n"
            s_file += [f"Sources to standardplot: {', '.join(self.sources_stdplot)}\n"]
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
