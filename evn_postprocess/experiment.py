#!/usr/bin/env python3
"""Defines an EVN experiment with all the relevant metadata required during the post-processing.


The metadata is obtained from different sources and/or at different stages of the post-processing.
This also keeps track of the steps that have been condducted in the post-processing so it can be
resumed, or restarted.
"""
import os
import copy
import numpy as np
import pickle
import json
import subprocess
import datetime as dt
from typing import Optional, Union, Iterable, Any, Generator
from pathlib import Path
from dataclasses import dataclass
from collections import defaultdict
from pyrap import tables as pt
from enum import Enum
from astropy import units as u
from rich import print as rprint
from rich import progress
import blessed
from . import environment as env
from . import dialog


def chunkert(pointer: int, total: int, step: int) -> Generator[tuple[int, int], Any, Any]:
    while pointer < total:
        n = min(step, total - pointer)
        yield (pointer, n)
        pointer = pointer + n


def percent(value: float, total: float) -> float:
    """Returns in percentage the given value as  100*value/total
    """
    return (value/total)*100.0


class Credentials(object):
    """Authentification for a given experiment. This class specifies two attributes:
        - username : str
        - password : str
    No restrictions on length/format for them. Once set, they cannot be modified
    (a new object needs to be created).
    """
    @property
    def username(self) -> Optional[str]:
        return self._username

    @property
    def password(self) -> Optional[str]:
        return self._password

    def __init__(self, username: Optional[str], password: Optional[str]):
        self._username = username
        self._password = password

    def __iter__(self) -> Generator[tuple[str, str], Any, Any]:
        for key in ('username', 'password'):
            yield key, getattr(self, key)


    def json(self) -> dict[str, str]:
        """Returns a dict with all attributes of the object.
        I define this method to use instead of .__dict__ as the later only reporst
        the internal variables (e.g. _username instead of username) and I want a better
        human-readable output.
        """
        d = dict()
        for key, val in self.__iter__():
            d[key] = val if val is not None else ''

        return d


class FlagWeight(object):
    """Stores the weight threshold applied (or to be applied) to the data during flagging
    and the percentage of data that have been flagged. These are the values used/obatined
    from flagweight.py at due time. Contains two properties:
    - threshold : float
        Threshold value set to flag visibilities with a weight below that value.
    - percentage : float
        Percentage of (non-zero) visibilities that were flagged.
        -1 value if not known.
    """
    @property
    def threshold(self) -> float:
        """Threshold value set to flag visibilities with a weight below such value
        when using flag_weight.py.
        """
        return self._th

    @threshold.setter
    def threshold(self, value: float):
        self._th = value

    @property
    def percentage(self) -> float:
        """Percentage of (non-zero) visibilities that have been flagged when running
        flag_weights.py. A value of -1 means that the amount is not known (e.g. the
        script has not been executed yet).
        """
        return self._pc

    @percentage.setter
    def percentage(self, value: float):
        self._pc = value

    def __init__(self, threshold: float, percentage: float = -1):
        self.threshold = threshold
        self.percentage = percentage

    def __iter__(self) -> Generator[tuple[str, float], Any, Any]:
        for key in ('threshold', 'percentage'):
            yield key, getattr(self, key)

    def json(self) -> dict[str, float]:
        """Returns a dict with all attributes of the object.
        I define this method to use instead of .__dict__ as the later only reporst
        the internal variables (e.g. _username instead of username) and I want a better
        human-readable output.
        """
        d = dict()
        for key, val in self.__iter__():
            d[key] = val

        return d


class SourceType(Enum):
    target = 0
    calibrator = 1
    fringefinder = 2
    other = 3


class Source(object):
    """Defines a source by name, type (i.e. target, reference, fringefinder, other)
    and if it must be protected or not (password required to get its data).
    """
    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> SourceType:
        return self._type

    @type.setter
    def type(self, new_type: SourceType):
        assert isinstance(new_type, SourceType)
        self._type = new_type

    @property
    def protected(self) -> bool:
        return self._protected

    @protected.setter
    def protected(self, to_be_protected: bool):
        assert isinstance(to_be_protected, bool)
        self._protected = to_be_protected

    def __init__(self, name: str, sourcetype: SourceType, protected: bool):
        assert isinstance(name, str), f"The name of the source must be a string (currrently {name})"
        assert isinstance(sourcetype, SourceType), \
               f"The name of the source must be a SourceType object (currrently {sourcetype})"
        assert isinstance(protected, bool), \
               f"The name of the source must be a boolean (currrently {protected})"
        self._name = name
        self._type = sourcetype
        self._protected = protected

    def __iter__(self) -> Generator[tuple[str, Union[str, SourceType, bool]], Any, Any]:
        for key in ('name', 'type', 'protected'):
            yield key, getattr(self, key)

    def json(self) -> dict[str, Union[str,bool]]:
        """Returns a dict with all attributes of the object.
        I define this method to use instead of .__dict__ as the later only reporst
        the internal variables (e.g. _username instead of username) and I want a better
        human-readable output.
        """
        d: dict[str, Union[str, bool]] = dict()
        for key, val in self.__iter__():
            if isinstance(val, SourceType):
                d[key] = val.name
            else:
                d[key] = val

        return d


@dataclass
class Antenna:
    name: str
    scheduled: bool = True
    observed: bool = False
    subbands: tuple = tuple()
    polswap: bool = False
    polconvert: bool = False
    onebit: bool = False
    logfsfile: bool = False
    antabfsfile: bool = False
    opacity: bool = False  # if data have opacity correction in the ANTAB file


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

    def json(self) -> dict[str, dict[str, Any]]:
        """Returns a dict with all attributes of the object.
        I define this method to use instead of .__dict__ as the later only reporst
        the internal variables (e.g. _username instead of username) and I want a better
        human-readable output.
        """
        d = dict()
        for ant in self.__iter__():
            d['Antenna'] = ant.__dict__

        return d


class Subbands(object):
    """Defines the frequency setup of a given observation with the following data:
        - n_subbands :  int
            Number of subbands.
        - channels : int
            Number of channels per subband.
        - frequencies : array-like
            Reference frequency for each channel and subband (NxM array, with N
            number of subbands, and M number of channels per subband).
        - bandwidths : astropy.units.Quantity or float
            Total bandwidth for each subband.
    """
    @property
    def n_subbands(self) -> int:
        return self._n_subbands

    @property
    def channels(self) -> int:
        return self._channels

    @property
    def frequencies(self) -> np.ndarray:
        return self._freqs

    @property
    def bandwidths(self) -> u.Quantity:
        return self._bandwidths

    def __init__(self, chans: int, freqs: np.ndarray, bandwidths: Union[float, u.Quantity]):
        """Inputs:
            - chans : int
                Number of channels per subband.
            - freqs : array-like
                Reference frequency for each channel and subband (NxM array, M number
                of channels per subband.
            - bandwidths : float or astropy.units.Quantity
                Total bandwidth for each subband. If not units are provided, Hz are assumed.
        """
        self._n_subbands = freqs.shape[0]
        assert isinstance(bandwidths, float) or isinstance(bandwidths, u.Quantity), \
            f"Bandiwdth {bandwidths} is not a float or Quantity as expected " \
            f"(found type {type(bandwidths)})."
        assert freqs.shape == (self._n_subbands, chans)
        self._channels = int(chans)
        self._freqs = np.copy(freqs)
        if isinstance(bandwidths, float):
            self._bandwidths = bandwidths*u.Hz
        else:
            self._bandwidths = bandwidths


    def __iter__(self):
        for key in ('n_subbands', 'channels', 'bandwidths', 'frequencies'):
            yield key, getattr(self, key)


    def json(self) -> dict[str, Union[int, float, tuple]]:
        """Returns a dict with all attributes of the object.
        I define this method to use instead of .__dict__ as the later only reporst
        the internal variables (e.g. _username instead of username) and I want a better
        human-readable output.
        """
        d = dict()
        for key, val in self.__iter__():
            if isinstance(val, u.Quantity):
                d[key] = val.to(u.Hz).value
            elif isinstance(val, np.ndarray):
                d[key] = tuple(val)
            else:
                d[key] = val

        return d


class CorrelatorPass(object):
    """Defines one correlator pass for a given experiment.
    It contains all relevant information that is pass-depended, e.g. associated .lis and
    MS files, frequency setup, etc.
    """

    @property
    def lisfile(self) -> Path:
        """Returns the name of the .lis file (libpath.Path object) used for this correlator pass.
        """
        return self._lisfile

    @lisfile.setter
    def lisfile(self, new_lisfile: Union[str, Path]):
        if isinstance(new_lisfile, Path):
            self._lisfile = new_lisfile
        elif isinstance(new_lisfile, str):
            self._lisfile = Path(new_lisfile)
        else:
            raise TypeError("The new lisfile parameter needs to be either a str or Path object.")

    @property
    def msfile(self) -> Path:
        """Returns the name of the MS file (libpath.Path object) associated to this correlator pass.
        """
        return self._msfile


    @msfile.setter
    def msfile(self, new_msfile: Union[Path, str]):
        if isinstance(new_msfile, Path):
            self._msfile = new_msfile
        elif isinstance(new_msfile, str):
            self._msfile = Path(new_msfile)
        else:
            raise TypeError("The new lisfile parameter needs to be either a str or Path object.")


    @property
    def fitsidifile(self) -> str:
        """Returns the name of the FITS IDI files associated to this correlator pass.
        Note that this is the common name for all files (without the trailing number)
        """
        return self._fitsidifile


    @fitsidifile.setter
    def fitsidifile(self, newfitsidifile: str):
        self._fitsidifile = newfitsidifile


    @property
    def pipeline(self) -> bool:
        """If this pass should be pipelined.
        """
        return self._pipeline


    @pipeline.setter
    def pipeline(self, pipeline: bool):
        self._pipeline = pipeline


    @property
    def sources(self) -> list[Source]:
        """List of sources present in this correlator pass.
        """
        return self._sources


    @sources.setter
    def sources(self, list_of_sources: list[Source]):
        self._sources = list(list_of_sources)


    @property
    def antennas(self) -> Antennas:
        """List of antennas available in the experiment.
        """
        return self._antennas


    @antennas.setter
    def antennas(self, new_antennas: Antennas):
        self._antennas = new_antennas


    @property
    def flagged_weights(self) -> Optional[FlagWeight]:
        return self._flagged_weights


    @flagged_weights.setter
    def flagged_weights(self, flagweight: FlagWeight):
        self._flagged_weights: Union[FlagWeight,None] = flagweight


    @property
    def freqsetup(self) -> Optional[Subbands]:
        return self._freqsetup


    @freqsetup.setter
    def freqsetup(self, new_freqsetup: Subbands):
        """Sets the frequency setup for the given correlator pass.
        """
        self._freqsetup: Union[Subbands, None] = new_freqsetup


    def __init__(self, lisfile: str, msfile: str, fitsidifile: str, pipeline: bool = True,
                 antennas: Optional[Antennas] = None,
                 flagged_weights: Optional[FlagWeight] = None):
        self._lisfile = Path(lisfile)
        self._msfile = Path(msfile)
        self._fitsidifile = fitsidifile
        self._sources = []
        self._pipeline = pipeline
        self._freqsetup = None  # Must be an object with subbands, freqs, channels, pols.
        if antennas is None:
            self._antennas = Antennas()
        else:
            self._antennas = antennas

        self._flagged_weights = flagged_weights


    def __iter__(self):
        for key in ('lisfile', 'msfile', 'fitsidifile', 'pipeline', 'sources', 'antennas',
                    'flagged_weights', 'freqsetup'):
            yield key, getattr(self, key)


    def json(self) -> dict[str, Any]:
        """Returns a dict with all attributes of the object.
        I define this method to use instead of .__dict__ as the later only reporst
        the internal variables (e.g. _username instead of username) and I want a better
        human-readable output.
        """
        d = dict()
        for key, val in self.__iter__():
            if hasattr(val, 'json'):
                d[key] = val.json()
            elif isinstance(val, Path):
                d[key] = str(val)
            elif isinstance(val, list):
                d[key] = [v.json() for v in val]
            else:
                d[key] = val

        return d


class Experiment(object):
    """Defines and EVN experiment with all relevant metadata.
    """
    @property
    def expname(self) -> str:
        """Name of the EVN experiment, in upper case.
        """
        return self._expname


    @property
    def eEVNname(self) -> Optional[str]:
        """Name of the e-EVN run in case this experiment was observed in this mode.
        Otherwise returns None
        """
        return self._eEVN


    @eEVNname.setter
    def eEVNname(self, eEVNname: Optional[str]):
        self._eEVN = eEVNname


    @property
    def piname(self) -> list[str]:
        return self._piname


    @piname.setter
    def piname(self, new_piname: list[str]):
        self._piname = new_piname


    @property
    def email(self) -> list[str]:
        return self._email


    @email.setter
    def email(self, new_email: list[str]):
        self._email = new_email


    @property
    def supsci(self) -> str:
        return self._supsci


    @supsci.setter
    def supsci(self, supsci: str):
        self._supsci = supsci


    @property
    def obsdate(self) -> str:
        """Epoch at which the EVN experiment was observed (starting date), in YYMMDD format.
        """
        return self._obsdate


    @obsdate.setter
    def obsdate(self, obsdate: str):
        self._obsdate = obsdate


    @property
    def obsdatetime(self) -> dt.datetime:
        """Epoch at which the EVN experiment was observed (starting date), in datetime format.
        """
        return dt.datetime.strptime(self.obsdate, '%y%m%d')


    @property
    def timerange(self) -> tuple[Optional[dt.datetime], Optional[dt.datetime]]:
        """Start and end time of the observation in datetime format.
        """
        return (self._startime, self._endtime)


    @timerange.setter
    def timerange(self, times: tuple[dt.datetime, dt.datetime]):
        """Start and end time of the observation in datetime format.
        Input:
            - times : tuple of datetime
                Tupple with (startime, endtime), each of them in datetime format.
        """
        starttime, endtime = times
        assert isinstance(starttime, dt.datetime)
        assert isinstance(endtime, dt.datetime)
        self._startime: Union[dt.datetime, None] = starttime
        self._endtime: Union[dt.datetime, None] = endtime


    @property
    def sources(self) -> list:
        """List of sources observed in the experiment.
        """
        return self._sources


    @sources.setter
    def sources(self, new_sources: list):
        """List of sources observed in the experiment.
        """
        self._sources = copy.deepcopy(new_sources)


    @property
    def antennas(self) -> Antennas:
        """List of antennas that were scheduled during the experiment.
        """
        return self._antennas


    @antennas.setter
    def antennas(self, new_antennas: Antennas):
        self._antennas = new_antennas


    @property
    def sources_stdplot(self) -> list[str]:
        """Returns the source names to be used to create the standardplots.
        If not specified manually, it will take all fringe-finders that are included in the
        list of sources.
        """
        if self._src_stdplot is None:
            src_list = []
            for a_source in self.sources:
                if a_source.type == SourceType.fringefinder:
                    src_list.append(a_source.name)

            return src_list
        else:
            return self._src_stdplot


    @sources_stdplot.setter
    def sources_stdplot(self, stdplot_sources: list[str]):
        self._src_stdplot: Union[list[str], None] = copy.deepcopy(stdplot_sources)


    @property
    def refant(self) -> list:
        """The antenna name to be used as reference. It can be either only one or multiple antennas.
        Returns a list object.
        """
        return self._refant


    @refant.setter
    def refant(self, new_refant: Union[list, str]):
        if isinstance(new_refant, list):
            self._refant = list(new_refant)
        elif isinstance(new_refant, str):
            self._refant = [refant.strip() for refant in new_refant.split(',')]
        else:
            raise TypeError(f"{new_refant} has an unrecognized type " \
                            "(str or list of strings expected)")


    @property
    def correlator_passes(self) -> list[CorrelatorPass]:
        """List of all correlator passes (one or more) that have been conducted.
        Each element of the list is a CorrelatorPass object with all the relevant
        associated information that may vary for each pass.
        The order of the elements is relevant as the first one is considered the
        reference pass (e.g. the one to produce the *_1_1.IDI files).
        """
        return self._passes


    @correlator_passes.setter
    def correlator_passes(self, new_passes: list[CorrelatorPass]):
        self._passes = copy.deepcopy(new_passes)


    def add_pass(self, a_new_pass: CorrelatorPass):
        """Appends a new correlator pass to the existing list of passes associated
        to this experiment.
        Input:
            a_new_pass : CorrelatorPass
        """
        assert isinstance(a_new_pass, CorrelatorPass)
        self._passes.append(a_new_pass)


    @property
    def credentials(self) -> Credentials:
        """Username and password to access the experiment data from the EVN
        archive during the proprietary period.
        """
        return self._credentials


    def set_credentials(self, username: Optional[str], password: Optional[str]):
        self._credentials = Credentials(username, password)


    @property
    def cwd(self) -> Path:
        """Returns the Path to the folder in eee where the experiment is being post-processed.
        """
        return self._cwd


    @property
    def special_params(self) -> dict[str, list[str]]:
        """Collects some special parameters (non-default ones) that should be used in
        given functions that run during the post-processing of the experiment.

        NOTE: This is a function that only exists to allow a better process of the experiment,
        not because it should be in this class.

        Returns a dict with the name of the function as key and a list of the parameters to use.
        Or None if no parameters have been set for this experiment.
        """
        return self._special_pars


    @special_params.setter
    def special_params(self, new_param: dict[str, list[str]]):
        self._special_pars.update(new_param)


    @property
    def last_step(self) -> Optional[str]:
        """Returns the last post-processing step that did run properly in a tentative previous run.
        """
        return self._last_step


    @last_step.setter
    def last_step(self, last_step: Union[str, None]):
        self._last_step: Union[str, None] = last_step


    @property
    def gui(self) -> dialog.Dialog:
        """Returns the GUI object that allows to exchange dialogs with the user
        """
        return self._gui


    @gui.setter
    def gui(self, gui_object: dialog.Dialog):
        self._gui = gui_object


    @property
    def silent_mode(self) -> bool:
        """Returns if the user wants to avoid opening graphical windows as for standard plots.
        True means that plots will not be automatically openned.
        """
        return self._silent

    @silent_mode.setter
    def silent_mode(self, do_silently: bool):
        self._silent = do_silently

    @property
    def graphics(self) -> bool:
        """Returns if the user wants to avoid launching graphical stuff (like opening plots).
        True means no graphical opens should take place.
        False if the user does not mind.
        """
        return self._graphics


    @graphics.setter
    def graphics(self, do_graphics: bool):
        self._graphics = do_graphics


    def __init__(self, expname: str, support_scientist: str):
        """Initializes an EVN experiment with the given name.

        Inputs:
        - expname : str
               The name of the experiment (case insensitive).
        """
        self._expname = expname.upper()
        self._eEVN = None
        self._piname = []
        self._email = []
        self._supsci = support_scientist.lower()
        self._obsdate = ''
        self._refant = []
        self._src_stdplot = None
        # TODO: verify this is the path and ask the user if different
        self._cwd = Path().cwd()
        if self._cwd != Path(f"/data0/{self.supsci}/{self.expname}"):
            rprint("\n\n[yellow]The current directory is not a default "
                   "one for an experiment.[/yellow]")
            answer = input(f"Do you want to change to /data0/{self.supsci}/{self.expname}?  (y/Y)")
            if answer.lower().strip() in ('y', 'yes'):
                self._cwd = Path(f"/data0/{self.supsci}/{self.expname}")
                self._cwd.mkdir(parents=True, exist_ok=True)
                os.chdir(self._cwd)

        # Attributes not known until the MS file is created
        self._startime = None
        self._endtime = None
        self._sources = []
        self._antennas = Antennas()
        self._credentials = Credentials(None, None)
        self._passes = []
        logpath = self.cwd / "logs"
        logpath.mkdir(parents=True, exist_ok=True)
        self._logs = {'dir': logpath, 'file': self.cwd / "processing.log"}
        self._checklist: dict[str, bool] = {}
        self._local_copy = self.cwd / f"{self.expname.lower()}.obj"
        self.parse_masterprojects()
        self._special_pars: dict[str, list[str]] = {}
        self._last_step = None
        self.gui = dialog.Terminal()
        self._silent = False
        self._graphics = True
        if not self._logs['file'].exists():
            # Writes down some snippets for jplotter in case the standard one fails.
            with open(self._logs['file'], 'a') as logfile:
                logfile.write("This is the log file for the Post-Processing of the EVN " \
                              f"experiment {self._expname}, observed on "\
                              f"{self.obsdatetime.strftime('%d %b %Y')}.\n")
                logfile.write("The associated JIVE support scientist is " \
                              f"{self._supsci.capitalize()}.\n\n")
                logfile.write("# Some shortcuts to run manually the standardplots in JPlotter:\n")
                logfile.write(f"ms {self._expname.lower()}.ms\nindexr\nlistr\nr\n\n")
                logfile.write("# Weight plot:\n")
                logfile.write("bl auto;fq */p;sort bl sb;pt wt;ckey sb sb[none]=1;ptsz 4;pl\n")
                logfile.write(f"save {self._expname.lower()}-weight.ps\n\n")
                logfile.write("# Amp & phase VS time plots:\n")
                logfile.write("bl Ef* -auto;fq 5/p;ch 0.1*last:0.9*last;avc vector;nxy 1 4; " \
                              "pt anptime;ckey src src[none]=1;y local;ptsz 2;time none;pl\n")
                logfile.write(f"save {self._expname.lower()}-ampphase-0.ps\n")
                logfile.write("time $start to +50m;pl\n")
                logfile.write(f"save {self._expname.lower()}-ampphase-1.ps\n\n")
                logfile.write("# Auto-correlation plots:\n")
                logfile.write("scan 1;bl auto;fq */p;ch none;avt vector;avc none;pt ampfreq;ckey" \
                              " p p[none]=1;sort bl;new sb false;multi true;y 0 1.6;nxy 2 4;pl\n")
                logfile.write(f"save {self._expname.lower()}-auto-0.ps\n")
                logfile.write("scan 91;pl\n")
                logfile.write(f"save {self._expname.lower()}-auto-1.ps\n\n")
                logfile.write("# Cross-correlation plots:\n")
                logfile.write("scan 1;pt anpfreq;bl Ef* -auto;fq *;ckey p['RR']=2 p['LL']=3 " \
                              "p['RL']=4 p['LR']=5;nxy 2 3;y local;draw lines points;multi " \
                              "true;new sb false;ptsz 4;sort bl sb;pl\n")
                logfile.write(f"save {self._expname.lower()}-cross-0.ps\n")
                logfile.write("scan 91;pl\n")
                logfile.write(f"save {self._expname.lower()}-cross-1.ps\n\n")
                logfile.write("exit\n")


    def get_setup_from_ms(self):
        """Obtains the time range, antennas, sources, and frequencies of the observation
        from all existing passes with MS files and incorporate them into the current object.
        """
        for a_pass in self.correlator_passes:
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
                        for (start, nrow) in chunkert(0, len(ms), 100):
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


    def parse_expsum(self):
        """Parses the .expsum file associated to the experiment to get different
        valuable data as:
            PI/contact-author name(s) and emails.
            Number of correlator passes.
            Sources to be observed.
        """
        with open(self.expsum, 'r') as expsum:
            expsumlines = expsum.readlines()
            sources = []
            for a_line in expsumlines:
                if 'Principal Investigator:' in a_line:
                    # The line is expected to be 'Principal Investigator: SURNAME  (EMAIL)'
                    piname, email = a_line.split(':')[1].split('(')
                    if piname.strip() not in self.piname:
                        self.piname.append(piname.strip())
                        self.email.append(email.split(')')[0].strip())
                elif 'co-I information' in a_line:
                    # Typically it does not show the :
                    name, email = a_line.replace('co-I information', '').replace(':', '').split('(')
                    name = name.strip()
                    email = email.split(')')[0].strip()
                    if name not in self.piname:
                        self.piname.append(name)
                        self.email.append(email)
                elif 'scheduled telescopes' in a_line:
                    sched_antennas = a_line.split(':')[1].strip().split(' ')
                    # The antennas will likely not be defined at this point, it checks and adds it
                    saved_ants = self.antennas.scheduled
                    for ant in sched_antennas:
                        if ant in saved_ants:
                            self.antennas[ant].scheduled = True
                        else:
                            self.antennas.add(Antenna(name=ant, scheduled=True))

                        if 'onebit' in self.special_params:
                            for onebit_ant in self.special_params['onebit']:
                                self.antennas[onebit_ant.capitalize()].onebit = True
                elif 'correlator passes' in a_line:
                    # self.correlator_passes = [None]*int(a_line.split()[0])
                    pass
                elif 'src = ' in a_line:
                    # Line with src = NAME, type = TYPE (something), use = PROTECTED (something)
                    srcname, srctype, srcprot = a_line.split(',')
                    srcname = srcname.split('=')[1].strip()
                    if srcname not in [s.name for s in sources]:
                        srctype = srctype.split('=')[1].split('(')[0].strip()
                        srcprot = srcprot.split('=')[1].split('(')[0].strip()
                        if srctype == 'target':
                            srctype = SourceType.target
                        elif srctype == 'reference':
                            srctype = SourceType.calibrator
                        elif srctype == 'fringefinder':
                            srctype = SourceType.fringefinder
                        elif srctype == 'calibrator':
                            srctype = SourceType.fringefinder
                        else:
                            srctype = SourceType.other

                        if srcprot == 'YES':
                            srcprot = False
                        elif srcprot == 'NO':
                            srcprot = True
                        else:
                            raise ValueError(f"Unknown 'use' value ({srcprot}) found in expsum.")

                        sources.append(Source(srcname, srctype, srcprot))

        self.sources = sources


    def parse_masterprojects(self):
        """Obtains the observing epoch from the MASTER_PROJECTS.LIS located in ccc.
        In case of being an e-EVN experiment, it will add that information to self.eEVN.
        """
        cmd = f"grep {self.expname} /ccs/var/log2vex/MASTER_PROJECTS.LIS"
        process = subprocess.Popen(["ssh", "jops@ccs", cmd], shell=False, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        output = process.communicate()[0].decode('utf-8')
        if process.returncode != 0:
            raise ValueError(f"Errorcode {process.returncode} when reading MASTER_PROJECTS.LIS."
                             + f"\n{self.expname} is probably not in the EVN database.")

        if output.count('\n') == 2:
            # It is an e-EVN experiment!
            # One line will have EXP EPOCH.
            # The other one eEXP EPOCH EXP1 EXP2..
            inputs = [i.split() for i in output[:-1].split('\n')]
            obsdate = ''
            for an_input in inputs:
                if an_input[0] == self.expname:
                    obsdate = an_input[1]
                else:
                    # The first element is the expname of the e-EVN run
                    self.eEVNname = an_input[0]

            self.obsdate = obsdate[2:]
        elif output.count('\n') == 1:
            expline = output[:-1].split()
            if len(expline) > 2:
                # This is an e-EVN, this experiment was the first one (so e-EVN is called the same)
                self.eEVNname = expline[0].strip()
            else:
                self.eEVNname = None

            self.obsdate = expline[1].strip()[2:]
        else:
            raise ValueError(f"{self.expname} not found in (ccs) MASTER_PROJECTS.LIS"
                             + "or server not reachable.")


    @property
    def vix(self) -> Path:
        """Returns the (Path object) to the .vix file related to the experiment.
        If the file does not exist in the experiment dir (in eee), is retrieved from ccs.
        """
        vixfilepath = Path(f"{self.expname}.vix")
        ename = self.expname if self.eEVNname is None else self.eEVNname
        if not vixfilepath.exists():
            env.scp(f"jops@ccs:/ccs/expr/{ename.upper()}/{ename.lower()}.vix", '.')
            self.log(f"scp jops@ccs:/ccs/expr/{ename.upper()}/{ename.lower()}.vix " \
                     f"{self.expname.lower()}.vix")
            os.symlink(f"{ename.lower()}.vix", f"{self.expname}.vix")
            self.log(f"ln -s {ename.lower()}.vix {self.expname}.vix")

        return vixfilepath


    @property
    def expsum(self) -> Path:
        """Returns the (Path object) to the .expsum file related to the experimet.
        If the files does not exist in the experiment dir (in eee), is retrieved from archive.
        """
        expsumfilepath = self.cwd / f"{self.expname.lower()}.expsum"
        if not expsumfilepath.exists():
            env.scp(f"jops@archive.jive.eu:piletters/{self.expname.lower()}.expsum", '.')
            self.log(f"scp jops@archive.jive.eu:piletters/{self.expname.lower()}.expsum .")

        return expsumfilepath


    @property
    def piletter(self) -> Path:
        """Returns the (Path object) to the .piletter file related to the experimet.
        If the files does not exist in the experiment dir (in eee), is retrieved from archive.
        """
        piletterpath = self.cwd / f"{self.expname.lower()}.piletter"
        if not piletterpath.exists():
            env.scp(f"jops@archive.jive.eu:piletters/{self.expname.lower()}.piletter", '.')
            self.log(f"scp jops@archive.jive.eu:piletters/{self.expname.lower()}.piletter .")

        return piletterpath


    @property
    def keyfile(self) -> Path:
        """Returns the (Path object) to the .key file related to the experiment.
        If the file does not exist in the experiment dir (in eee), is retrieved from vlbeer.
        """
        keyfilepath = self.cwd / f"{self.expname.lower()}.key"
        if not keyfilepath.exists():
            try:
                env.scp(f"evn@vlbeer.ira.inaf.it:vlbi_arch/" \
                        f"{self.obsdatetime.strftime('%b%y').lower()}/{self.expname.lower()}.key", \
                        ".", timeout=120)
                self.log(f"scp evn@vlbeer.ira.inaf.it:vlbi_arch/" \
                         f"{self.obsdatetime.strftime('%b%y').lower()}/" \
                         f"{self.expname.lower()}.key .")
            except subprocess.TimeoutExpired:
                self.log("Could not retrieve the key file from vlbeer. Check the connection and "
                         "do it manually if you want the key file.")
                rprint("\n[bold yellow]Could not retrieve the key file from vlbeer.[/bold yellow]")
                # Because a zero-sized file will be there
                keyfilepath.unlink(missing_ok=True)
            except ValueError:
                self.log("Could not find the key file in vlbeer.")
                rprint("\n[bold yellow]Could not find the key file in vlbeer.[/bold yellow]")
                keyfilepath.unlink(missing_ok=True)

        return keyfilepath


    @property
    def sumfile(self) -> Path:
        """Returns the (Path object) to the .sum file related to the experiment.
        If the file does not exist in the experiment dir (in eee), is retrieved from vlbeer.
        """
        sumfilepath = self.cwd / f"{self.expname.lower()}.sum"
        if not sumfilepath.exists():
            try:
                env.scp(f"evn@vlbeer.ira.inaf.it:vlbi_arch/" \
                        f"{self.obsdatetime.strftime('%b%y').lower()}/{self.expname.lower()}.sum", \
                        ".", timeout=120)
                self.log(f"scp evn@vlbeer.ira.inaf.it:vlbi_arch/" \
                         f"{self.obsdatetime.strftime('%b%y').lower()}/" \
                         f"{self.expname.lower()}.sum .")
            except subprocess.TimeoutExpired:
                self.log("Could not retrieve the key file from vlbeer. Check the connection and "
                         "do it manually if you want the key file.")
                rprint("\n[bold yellow]Could not retrieve the key file from vlbeer.[/bold yellow]")
                # Because a zero-sized file will be there
                sumfilepath.unlink(missing_ok=True)
            except ValueError:
                self.log("Could not find the sum file in vlbeer.")
                rprint("\n[bold yellow]Could not find the sum file in vlbeer.[/bold yellow]")
                sumfilepath.unlink(missing_ok=True)

        return sumfilepath


    @property
    def logfile(self) -> dict:
        """Returns a dict with the logs, with two keys:
        - 'dir': the directory where individual log files can be stored (by default 'logs/')
        - 'file': the 'processing.log' file which stores all steps that run during
                  the post-processing of the experiment.
        """
        return self._logs


    def log(self, entry: str, timestamp: bool = False):
        """Writes into the processing.log file a new entry.
        """
        if timestamp:
            cmd = f"# {dt.datetime.today().strftime('%d-%m-%Y %H:%M')}\n{entry}\n"
        else:
            cmd = f"{entry}\n"

        with open(self.logfile['file'], 'a') as logfile:
            logfile.write(cmd)


    @property
    def checklist(self) -> dict[str, bool]:
        return self._checklist


    def update_checklist(self, a_step: str, is_done: bool = True):
        """Updates the step in the checklist and marks it as done or not (True/False,
        as specified in is_done). If a_step does not exist, it will raise a ValueError Exception.
        """
        if a_step not in self._checklist:
            raise ValueError(f"The step {a_step} is not present in the checklis of {self.expname}.")

        self._checklist[a_step] = is_done


    def feedback_page(self) -> str:
        """Returns the url link to the station feedback pages for the experiment.
        """
        if self.eEVNname is not None:
            return " -- No associated feedback pages --"

        # Folling back the month to the standard session: feb, jun, or oct:
        if self.obsdatetime.month // 10 > 0:
            sess_month = 'oct'
        elif self.obsdatetime.month // 6 > 0:
            sess_month = 'jun'
        elif self.obsdatetime.month // 2 > 0:
            sess_month = 'feb'
        else:
            # It can be an out-of-session experiment or an e-EVN with a single experiment
            return " -- No associated feedback pages --"

        return f"https://services.jive.eu/top/Feedback/experiment/{self.expname.upper()}"


    @property
    def archive_page(self) -> str:
        """Returns the url link to the EVN Archive pages for the experiment.
        """
        return f"https://archive.jive.eu/scripts/arch.php?exp={self.expname.upper()}"


    def exists_local_copy(self) -> bool:
        """Checks if there is a local copy of the Experiment object stored in a local file.
        """
        return self._local_copy.exists()


    def store(self, path: Optional[Path] = None):
        """Stores the current Experiment into a file in the indicated path. If not provided,
        it will be '.{expname.lower()}.obj' where exp is the name of the experiment.
        """
        # self.store_json(path)
        if path is not None:
            self._local_copy = path

        with open(self._local_copy, 'wb') as f:
            pickle.dump(self, f)


    def store_json(self, path: Optional[Path] = None):
        """Stores the current Experiment into a JSON file.
        If path not provided, it will be '{expname.lower()}.json'.
        """
        if path is not None:
            self._local_copy = path

        # TODO: do the json storing
        # with open(self._local_copy, 'wb') as f:
        #     json.dump(self.json(), f, cls=ExpJsonEncoder, indent=4)


    def load(self, path: Optional[Path] = None):
        """Loads the current Experiment that was stored in a file in the indicated path.
        If path is None, it assumes the standard path of '.{exp}.obj' where 'exp' is the name
        of the experiment.
        """
        if path is not None:
            self._local_copy = path

        with open(self._local_copy, 'rb') as f:
            obj = pickle.load(f)
            # obj = json.load(f, cls=ExpJsonEncoder)

        return obj


    def __repr__(self, *args, **kwargs) -> str:
        rep = super().__repr__(*args, **kwargs)
        rep.replace("object", f"object ({self.expname})")
        return rep


    def __str__(self) -> str:
        return f"<Experiment {self.expname}>"


    def __iter__(self):
        for key in ('expname', 'eEVNname', 'piname', 'email', 'supsci', 'obsdate', 'obsdatetime',
                    'timerange', 'sources', 'sources_stdplot', 'antennas', 'refant', 'credentials',
                    'cwd', 'logfile', 'vix', 'expsum', 'special_params',
                    'last_step', 'gui', 'correlator_passes'):
            yield key, getattr(self, key)


    def json(self) -> dict:
        """Returns a dict with all attributes of the object.
        I define this method to use instead of .__dict__ as the later only reporst
        the internal variables (e.g. _username instead of username) and I want a better
        human-readable output.
        """
        d = dict()
        for key, val in self.__iter__():
            if hasattr(val, 'json'):
                d[key] = val.json()
            elif isinstance(val, Path):
                d[key] = val.name
            elif isinstance(val, dt.datetime):
                d[key] = val.strftime('%Y-%m-%d')
            elif isinstance(val, dt.date):
                d[key] = val.strftime('%Y-%m-%d')
            elif isinstance(val, list) and (len(val) > 0) and hasattr(val[0], 'json'):
                d[key] = [v.json() for v in val]
            elif isinstance(val, tuple) and (len(val) > 0) and isinstance(val[0], dt.datetime):
                d[key] = [v.strftime('%Y-%m-%d %H:%M:%S') for v in val]
            elif isinstance(val, dict):
                d[key] = {}
                for k, v in val:
                    if hasattr(v, 'json'):
                        d[key][k] = v.json()
                    elif hasattr(v, 'name'):
                        d[key][k] = v.name
                    else:
                        d[key][k] = v
            else:
                d[key] = val

        return d


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


    def print_blessed(self, outputfile=None):
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


class ExpJsonEncoder(json.JSONEncoder):
    """Encodes properly the Experiment class to be able to be written as a JSON format
    """
    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, dt.date):
            return obj.strftime('%Y-%m-%d')
        elif hasattr(obj, 'json'):
            return obj.json()
        elif isinstance(obj, Path):
            return obj.name
        elif isinstance(obj, np.ndarray):
            return list(obj)
        else:
            print(obj)
            return json.JSONEncoder.default(self, obj)


