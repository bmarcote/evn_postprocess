#!/usr/bin/env python3
"""Defines an EVN experiment with all the relevant metadata required during the post-processing.


The metadata is obtained from different sources and/or at different stages of the post-processing.
This also keeps track of the steps that have been condducted in the post-processing so it can be
resumed, or restarted.
"""
import os
import numpy as np
import pickle
import subprocess
import datetime as dt
import subprocess
from datetime import datetime as dt
from dataclasses import dataclass
from pyrap import tables as pt
from enum import Enum
from astropy import units as u
import environment as env


class Credentials(object):
    """Authentification for a given experiment. This class specifies two attributes:
        - username : str
        - password : str
    No restrictions on length/format for them. Once set, they cannot be modified
    (a new object needs to be created).
    """
    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password



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
    def threshold(self):
        """Threshold value set to flag visibilities with a weight below such value
        when using flag_weight.py.
        """
        return self._th

    @threshold.setter
    def threshold(self, value):
        self._th = value

    @property
    def percentage(self):
        """Percentage of (non-zero) visibilities that have been flagged when running
        flag_weights.py. A value of -1 means that the amount is not known (e.g. the
        script has not been executed yet).
        """
        return self._pc

    @percentage.setter
    def percentage(self, value):
        self._pc = value


    def __init__(self, threshold: float, percentage: float = -1):
        self.threshold = threshold
        self.percentage = percentage



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
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def protected(self):
        return self._protected

    # @protected.setter
    # def protected(self, tobeprotected):
    #     self._protected = tobeprotected

    def __init__(self, name: str, sourcetype: SourceType, protected: bool):
        assert isinstance(name, str), f"The name of the source must be a string (currrently {name})"
        assert isinstance(sourcetype, SourceType), \
               f"The name of the source must be a SourceType object (currrently {sourcetype})"
        assert isinstance(protected, bool), f"The name of the source must be a boolean (currrently {protected})"
        self._name = name
        self._type = sourcetype
        self._protected = protected



@dataclass
class Antenna:
    name: str
    scheduled: bool = True
    observed: bool = False
    # reference: bool = False
    polswap: bool = False
    polconvert: bool = False
    onebit: bool = False
    logfsfile: bool = False
    antabfsfile: bool = False



class Antennas(objects):
    """List of antennas (Antenna class)
    """
    def __init__(self, antennas=None):
        if antennas is not None:
            self._antennas = antennas[:]
        else:
            self._antennas = []

    def add(self, new_antenna):
        assert isinstance(new_antenna, Antenna)
        self._antenna.append(new_antenna)

    def names(self):
        return [a.name for a in self._antennas]

    def scheduled(self):
        return [a.name for a in self._antennas if a.scheduled]

    def observed(self):
        return [a.name for a in self._antennas if a.observed]

    # def reference(self):
    #     return [a.name for a in self._antennas if a.reference]

    def polswap(self):
        return [a.name for a in self._antennas if a.polswap]

    def polconvert(self):
        return [a.name for a in self._antennas if a.polconvert]

    def onebit(self):
        return [a.name for a in self._antennas if a.onebit]

    def logfsfile(self):
        return [a.logfsfile for a in self._antennas if a.logfsfile]

    def antabfsfile(self):
        return [a.antabfsfile for a in self._antennas if a.antabfsfile]

    def __len__(self):
        return len(self._antennas)

    def __getitem__(self, key):
        return self._antennas[self.names.index(key)]

    def __delitem__(self, key):
        return self._antennas.remove(self.names.index(key))

    def __iter__(self):
        return self._antennas.__iter__()

    def __reversed__(self):
        return self._antennas[::-1]

    def __contains__(self, key):
        return key in self.names



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
    def n_subbands(self):
        return self._n_subbands

    @property
    def channels(self):
        return self._channels

    @property
    def frequencies(self):
        return self._freqs

    @property
    def bandwidths(self):
        return self._bandwidths

    def __init__(self, chans: int, freqs, bandwidths):
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
        assert isinstance(chans, int)
        assert isinstance(bandwidths, float) or isinstance(bandwidths, u.Quantity)
        assert freqs.shape == (self._n_subbands, chans)
        self._channels = chans
        self._freqs = np.copy(freqs)
        if isinstance(bandwidths, float):
            self._bandwidths = bandwidths*u.Hz
        else:
            self._bandwidths = bandwidths



class CorrelatorPass(object):
    """Defines one correlator pass for a given experiment.
    It contains all relevant information that is pass-depended, e.g. associated .lis and
    MS files, frequency setup, etc.
    """

    @property
    def lisfile(self):
        """Returns the name of the .lis file (libpath.Path object) used for this correlator pass.
        """
        return self._lisfile

    @property
    def msfile(self):
        """Returns the name of the MS file (libpath.Path object) associated to this correlator pass.
        """
        return self._msfile

    @property
    def fitsidifile(self):
        """Returns the name of the FITS IDI files associated to this correlator pass.
        Note that this is the common name for all files (without the trailing number)
        """
        return self._fitsidifile

    @fitsidifile.setter
    def fitsidifile(self, newfitsidifile):
        self._fitsidifile = newfitsidifile

    @property
    def pipeline(self):
        """If this pass should be pipelined.
        """
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline):
        self._pipeline = pipeline

    @property
    def sources(self):
        """List of sources present in this correlator pass.
        """
        return self._sources

    @sources.setter
    def sources(self, list_of_sources):
        self._sources = tuple(list_of_sources)

    @property
    def antennas(self):
        """List of antennas available in the experiment.
        """
        return self._antennas

    @antennas.setter
    def antennas(self, new_antennas):
        isinstance(new_antennas, Antennas)
        self._antennas = new_antennas

    @property
    def flagged_weights(self):
        return self._flagged_weights

    @flagged_weights.setter
    def flagged_weights(self, flagweight):
        assert isinstance(flagweight, FlagWeight)
        self._flagged_weights = flagweight

    @property
    def freqsetup(self):
        return self._freqsetup

    def freqsetup(self, channels, frequencies, bandwidths):
        """Sets the frequency setup for the given correlator pass.
        Inputs:
            - chans : int
                Number of channels per subband.
            - freqs : array-like
                Reference frequency for each channel and subband (NxM array, M number
                of channels per subband.
            - bandwidths : float or astropy.units.Quantity
                Total bandwidth for each subband. If not units are provided, Hz are assumed.
        """
        self._freqsetup = Subbands(channels, frequencies, bandwidths)

    def __init__(self, lisfile: str, msfile: str, fitsidifile: str, pipeline: bool = True,
                 antennas: Antennas = None, flagged_weights = None):
        self._lisfile = Path(lisfile)
        self._msfile = Path(msfile)
        self._fitsidifile = fitsidifile
        self._sources = None
        self._pipeline = pipeline
        self._freqsetup = None # Must be an object with subbands, freqs, channels, pols.
        if antennas is None:
            self._antennas = Antennas()
        else:
            self._antennas = antennas

        self._flagged_weights = flagged_weights




class Experiment(object):
    """Defines and EVN experiment with all relevant metadata.
    """
    @property
    def expname(self):
        """Name of the EVN experiment, in upper case.
        """
        return self._expname

    @property
    def eEVNname(self):
        """Name of the e-EVN run in case this experiment was observed in this mode.
        Otherwise returns None
        """
        return self._eEVN

    @eEVNname.setter
    def eEVNname(self, eEVNname):
        self._eEVN = eEVNname

    @property
    def piname(self):
        return self._piname

    @piname.setter
    def piname(self, new_piname):
        self._piname = new_piname

    @property
    def email(self):
        return self._email

    @email.setter
    def email(self, new_email):
        self._email = new_email

    @property
    def supsci(self):
        return self._supsci

    @supsci.setter
    def supsci(self, supsci):
        self._supsci = supsci

    @property
    def obsdate(self):
        """Epoch at which the EVN experiment was observed (starting date), in YYMMDD format.
        """
        return self._obsdate

    @obsdate.setter
    def obsdate(self, obsdate):
        self._obsdate = obsdate

    @property
    def obsdatetime(self):
        """Epoch at which the EVN experiment was observed (starting date), in datetime format.
        """
        return dt.datetime.strptime(self.obsdate, '%y%m%d')

    @property
    def timerange(self):
        """Start and end time of the observation in datetime format.
        """
        return self._startime, self._endtime

    @timerange.setter
    def timerange(self, times):
        """Start and end time of the observation in datetime format.
        Input:
            - times : tuple of datetime
                Tupple with (startime, endtime), each of them in datetime format.
        """
        starttime, endtime = times
        assert isinstance(starttime, dt.datetime)
        assert isinstance(endtime, dt.datetime)
        self._startime = starttime
        self._endtime = endtime

    @property
    def sources(self):
        """List of sources observed in the experiment.
        """
        return self._sources

    @sources.setter
    def sources(self, new_sources):
        """List of sources observed in the experiment.
        """
        self._sources = list(new_sources)

    @property
    def sources_stdplot(self):
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
    def sources_stdplot(self, stdplot_sources):
        self._src_stdplot = list(stdplot_sources)

    @property
    def refant(self):
        """The antenna name to be used as reference. It can be either only one or multiple antennas.
        Returns a list object.
        """
        return self._refant

    @refant.setter
    def refant(self, new_refant):
        if isinstance(new_refant, list):
            self._refant = list(new_refant)
        elif isinstance(new_refant, str):
            if ',' in new_refant:
                self._refant = [r.strip() for r in new_refant.split(',')]
            else:
                self._refant = [new_refant,]
        else:
            raise ValueError(f"{new_refant} has an unrecognized type (string or list of strings expected)")

    @property
    def correlator_passes(self):
        """List of all correlator passes (one or more) that have been conducted.
        Each element of the list is a CorrelatorPass object with all the relevant
        associated information that may vary for each pass.
        The order of the elements is relevant as the first one is considered the
        reference pass (e.g. the one to produce the *_1_1.IDI files).
        """
        return self._passes

    @correlator_passes.setter
    def correlator_passes(self, new_passes):
        assert isinstance(new_passes, list)
        self._passes = list(new_passes)

    def add_pass(self, a_new_pass):
        """Appends a new correlator pass to the existing list of passes associated
        to this experiment.
        Input:
            a_new_pass : CorrelatorPass
        """
        assert isinstance(a_new_pass, CorrelatorPass)
        self._passes.append(a_new_pass)

    @property
    def credentials(self):
        """Username and password to access the experiment data from the EVN
        archive during the proprietary period.
        """
        return self._credentials

    def set_credentials(self, username, password):
        self._credentials = Credentials(username, password)

    @property
    def cwd(self):
        """Returns the Path to the folder in eee where the experiment is being post-processed.
        """
        return Path(f"/data0/{self.supsci}/{self.expname}")

    @property
    def special_params(self):
        """Collects some special parameters (non-default ones) that should be used in determined functions
        that run during the post-processing of the experiment.

        NOTE: This is a function that only exists to allow a better process of the experiment, not because it
        should be in this class.

        Returns a dict with the name of the function as key and a list of the parameters to use.
        Or None if no parameters have been set for this experiment.
        """
        return self._special_pars

    @special_params.setter
    def special_params(self, new_param):
        isinstance(new_param, dict)
        self._special_pars.update(new_params)

    def __init__(self, expname, support_scientist):
        """Initializes an EVN experiment with the given name.

        Inputs:
        - expname : str
               The name of the experiment (case insensitive).
        """
        self._expname = expname.upper()
        self._eEVN = None
        self._piname = None
        self._email = None
        self._supsci = support_scientist.lower()
        self._obsdate = None
        self._refant = None
        self._src_stdplot = None
        # Attributes not known until the MS file is created
        self._startime = None
        self._endtime = None
        self._sources = None
        self._credentials = Credentials(None, None)
        self._passes = []
        logpath = self.cwd / "logs"
        logpath.mkdir(exists_ok=True)
        self._logs = {'dir': logpath, 'file': self.cwd / "processing.log"}
        self._checklist = {} # TODO: add here by default all steps in the check list, with False value
        self._local_copy = None
        self.parse_masterprojects()
        self._special_pars = None



    def get_setup_from_ms(self):
        """Obtains the time range, antennas, sources, and frequencies of the observation
        from all existing passes with MS files and incorporate them into the current object.
        """
        for i,a_pass in enumerate(self.passes):
            try:
                with pt.table(a_pass.msfile, readonly=True, ack=False) as ms:
                    with pt.table(ms.getkeyword('ANTENNA'), readonly=True, ack=False) as ms_ant:
                        for ant_name in ms_ant.getcol('NAME'):
                            if ant_name in a_pass.antennas.names:
                                self._passes[i].antennas[ant_name].observed = True
                            else:
                                ant = Antenna(name=ant_name, observed=True)
                                self._passes[i].antennas.add(ant)

                    with pt.table(ms.getkeyword('FIELD'), readonly=True, ack=False) as ms_field:
                        a_pass.sources = ms_field.getcol('NAME')

                    with pt.table(ms.getkeyword('OBSERVATION'), readonly=True, ack=False) as ms_obs:
                        self.timerange = dt.datetime(1858, 11, 17, 0, 0, 2) + \
                             ms_obs.getcol('TIME_RANGE')[0]*dt.timedelta(seconds=1)
                    with pt.table(ms.getkeyword('SPECTRAL_WINDOW'), readonly=True, ack=False) as ms_spw:
                        a_pass.freqsetup(ms_spw.getcol('NUM_CHAN')[0], ms_spw.getcol('CHAN_FREQ'),
                                         ms_spw.getcol('TOTAL_BANDWIDTH')[0,0])
            except RuntimeError:
                print(f"WARNING: {a_pass.msfile} not found.")


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
                    self.piname = piname.strip()
                    self.email = email.split(')')[0].strip()
                elif 'co-I information' in a_line:
                    # Typically it does not show the :
                    name,email = a_line.replace('co-I information','').replace(':','').split('(')
                    name = name.strip()
                    email = email.split(')')[0].strip()
                    if isinstance(self.piname, list):
                        self.piname += name
                        self.email += email
                    else:
                        self.piname = [self.piname, name]
                        self.email = [self.email, email]
                elif 'scheduled telescopes' in a_line:
                    sched_antennas = a_line.split(':')[1].split()
                    # The antennas will likely not be defined at this point, it checks it and adds it
                    saved_ants = self.correlator_passes.antennas.scheduled
                    for ant in sched_antennas:
                        if ant in saved_ants:
                            self.correlator_passes.antennas[ant].scheduled = True
                        else:
                            self.correlator_passes.antennas.add(Antenna(name=ant, scheduled=True))
                elif 'correlator passes' in a_line:
                    self.correlator_passes = int(a_line.split()[0])
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
                        else:
                            srctype = SourceType.other

                        if srcprot == 'YES':
                            srcprot = False
                        elif srcprot == 'NO':
                            srcprot = True
                        else:
                            raise ValueError(f"Unknown 'use' value ({srcprot}) found in the expsum.")

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
            raise ValueError(f"Errorcode {process.returncode} when reading MASTER_PROJECTS.LIS." \
                    + f"\n{self.expname} is probably not in the EVN database.")

        if output.count('\n') == 2:
            # It is an e-EVN experiment!
            # One line will have EXP EPOCH.
            # The other one eEXP EPOCH EXP1 EXP2..
            inputs = [i.split() for i in output[:-1].split('\n')]
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
            raise ValueError(f"{self.expname} not found in (ccs) MASTER_PROJECTS.LIS" \
                             + "or server not reachable.")


    @property
    def vix(self):
        """Returns the (Path object) to the .vix file related to the experiment.
        If the file does not exist in the experiment dir (in eee), is retrieved from ccs.
        """
        ename = self.expname if self.eEVNname is None else self.eEVNname
        vixfilepath = self.cwd / f"{ename.lower()}.vix"
        if not vixfilepath.exists():
            cmd, output = actions.scp(f"jops@ccs:/ccs/expr/{ename.upper()}/{ename.lower()}.vix", '.')
            self.log(f"scp jops@ccs:/ccs/expr/{ename.upper()}/{ename.lower()}.vix .", False)
        if not (self.cwd / f"{self.expname}.vix").exists():
            os.symlink(f"{ename.lower()}.vix", f"{self.expname}.vix")
            self.log(f"{ename.lower()}.vix {self.expname}.vix", False)

        return vixfilepath


    @property
    def expsum(self):
        """Returns the (Path object) to the .expsum file related to the experimet.
        If the files does not exist in the experiment dir (in eee), is retrieved from jop83.
        """
        expsumfilepath = self.cwd / f"{self.expname.lower()}.expsum"
        if not expsumfilepath.exists():
            cmd, output = actions.scp(f"jops@jop83:piletters/{self.expname.lower()}.expsum", '.')
            self.log(f"scp jops@jop83:piletters/{self.expname.lower()}.expsum .", False)

        return expsumfilepath


    @property
    def piletter(self):
        """Returns the (Path object) to the .piletter file related to the experimet.
        If the files does not exist in the experiment dir (in eee), is retrieved from jop83.
        """
        piletterpath = self.cwd / f"{self.expname.lower()}.piletter"
        if not piletterpath.exists():
            cmd, output = actions.scp(f"jops@jop83:piletters/{self.expname.lower()}.piletter", '.')
            self.log(f"scp jops@jop83:piletters/{self.expname.lower()}.piletter .", False)

        return piletterpath


    @property
    def logfile(self):
        """Returns a dict with the logs, with two keys:
        - 'dir': the directory where individual log files can be stored (by default 'logs/')
        - 'file': the 'processing.log' file which stores all steps that run during the post-processing
                  of the experiment.
        """
        return self._logs


    def log(self, entry, timestamp=True):
        """Writes into the processing.log file a new entry.
        """
        if timestamp:
            cmd = f"echo '# {dt.strftime('%d-%m-%Y %H:%M')}\n{entry}\n'"
        else:
            cmd = f"echo '{entry}\n'"

        env.shell_command(cmd, parameters=['>>', self.logfile['file']], shell=False)


    @property
    def checklist(self):
        return self._checklist


    def update_checklist(self, a_step, is_done=True):
        """Updates the step in the checklist and marks it as done or not (True/False, as specified in is_done)
        If a_step does not exist, it will raise a ValueError Exception.
        """
        if not a_step in self._checklist:
            raise ValueError(f"The step {a_step} is not present in the checklis of {self.expname}.")

        self._checklist[a_step] = is_done


    def store(self, path=None):
        """Stores the current Experiment into a file in the indicated path. If not provided,
        it will be '.{exp}.obj' where exp is the name of the experiment.
        """
        if path is None:
            path = self.cwd / f"{self.expname}.obj"
        self._local_copy = path
        with open(path, 'wb') as file:
            pickle.dump(self, file)


    def exists_local_copy(self):
        """Checks if there is a local copy of the Experiment object stored in a local file.
        """
        return (self._local_copy is not None) and self._local_copy.exists()


    def load(self, path=None):
        """Loads the current Experiment that was stored in a file in the indicated path. If path is None,
        it assumes the standard path of '.{exp}.obj' where exp is the name of the experiment.
        """
        if path is None:
            path = self.cwd / f"{self.expname}.obj"

        self._local_copy = path
        with open(path, 'wb') as file:
            obj = pickle.load(file)

        return obj





