"""Defines an EVN experiment with all the relevant metadata
required during the post-processing.

The metadata is obtained from different sources and/or at
different stages of the post-processing.
"""
import os
import numpy as np
import subprocess
import datetime as dt
from pyrap import tables as pt
from enum import Enum



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

    def __init__(self, username, password):
        self._username = username
        self._password = password


class SourceType(Enum):
    target = 0
    calibrator = 1
    fringefinder = 2
    other = 3


class Source(object):
    """Defines a source by name, type (i.e. target, reference, fringefinder)
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

    @protected.setter
    def protected(self, tobeprotected):
        self._protected = tobeprotected

    def __init__(self, name, sourcetype, protected):
        self._name = name
        self._type = sourcetype
        self.protected = protected


class Subbands(object):
    """Defines the frequency setup of a given observation with the following data:
        - n_subbands : float
            Number of subbands.
        - channels : array-like
            Number of channels per subband (N-array, with N number of subbands).
        - freqs : array-like
            Reference frequency for each channel and subband (NxM array, with N
            number of subbands, and M number of channels per subband).
        - bandwidths : array-like
            Total bandwidth for each subband (N array, with N number of subbands).

    """
    @property
    def n_subbands(self):
        return self._n_subbands

    @property
    def channels(self):
        return self._channels

    @property
    def freqs(self):
        return self._freqs

    @property
    def bandwidths(self):
        return self._bandwidths

    def __init__(self, chans, freqs, bandwidths):
        """Inputs:
            - chans : array-like
                Number of channels per subband (N-array, with N number of subbands).
            - freqs : array-like
                Reference frequency for each channel and subband (NxM array, M number
                of channels per subband.
            - bandwidths : array-like
                Total bandwidth for each subband (N array, with N number of subbands).
        """
        self._n_subbands = len(chans)
        assert self._n_subbands == len(bandwidths)
        assert freqs.shape == (self._n_subbands, chans[0])
        self._channels = np.copy(chans)
        self._freqs = np.copy(freqs)
        self._bandwidths = np.copy(bandwidths)


class CorrelatorPass(object):
    """Defines one correlator pass for a given experiment.
    It contains all relevant information that is pass-depended, e.g. associated .lis and
    MS files, frequency setup, etc.
    """

    @property
    def lisfile(self):
        """Returns the name of the .lis file used for this correlator pass.
        """
        return self._lisfile

    @property
    def msfile(self):
        """Returns the name of the MS file associated to this correlator pass.
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
    def freqsetup(self):
        return self._freqsetup

    def freqsetup(self, channels, frequencies, bandwidths):
        """Sets the frequency setup for the given correlator pass.
        Inputs:
            - channels : array-like
                Number of channels per subband (N-array, with N number of subbands).
            - frequencies : array-like
                Reference frequency for each channel and subband (NxM array, M number
                of channels per subband.
            - bandwidths : array-like
                Total bandwidth for each subband (N array, with N number of subbands).
        """
        self._freqsetup = Subbands(channels, frequencies, bandwidths)

    def __init__(self, lisfile, msfile, fitsidifile, pipeline=True):
        self._lisfile = lisfile
        self._msfile = msfile
        self._fitsidifile = fitsidifile
        self._sources = []
        self._pipeline = pipeline
        self._freqsetup = None # Must be an object with subbands, freqs, channels, pols.



class Experiment(object):
    """Defines and EVN experiment with all relevant metadata.
    """

    @property
    def expname(self):
        """Name of the EVN experiment, in upper cases.
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
    def antennas(self):
        """List of antennas available in the experiment.
        """
        return self._antennas

    @antennas.setter
    def antennas(self, new_antennas):
        self._antennas = tuple(new_antennas)

    @property
    def onebit_antennas(self):
        """List of antennas that recorded with 1 bit.
        """
        return self._onebit_antennas

    @onebit_antennas.setter
    def onebit_antennas(self, new_onebit_antennas):
        if new_onebit_antennas is None:
            self._onebit_antennas = ()
        else:
            self._onebit_antennas = tuple(new_onebit_antennas)

    @property
    def ref_antennas(self):
        """List of antennas to be used as reference in standardplots and Pipeline.
        """
        return self._ref_antennas

    @ref_antennas.setter
    def ref_antennas(self, new_ref_antennas):
        self._ref_antennas = tuple(new_ref_antennas)

    @property
    def polconvert_antennas(self):
        """List of antennas that require running PolConvert.
        """
        return self._polconvert_antennas

    @polconvert_antennas.setter
    def polconvert_antennas(self, new_list_antennas):
        self._polconvert_antennas = tuple(new_list_antennas)

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
    def ref_sources(self):
        """List of sources to be used for standardplots.
        """
        return self._ref_sources

    @ref_sources.setter
    def ref_sources(self, new_ref_sources):
        """List of sources to be used for standardplots.
        """
        self._ref_sources = list(new_ref_sources)

    @property
    def correlator_passes(self):
        "Number of correlator passes that have been performed."
        return self._number_passes

    @correlator_passes.setter
    def correlator_passes(self, number_passes):
        self._number_passes = number_passes

    @property
    def passes(self):
        """List of all correlator passes (one or more) that have been conducted.
        Each element of the list is a CorrelatorPass object with all the relevant
        associated information that may vary for each pass.
        The order of the elements is relevant as the first one is considered the
        reference pass (e.g. the one to produce the *_1_1.IDI files).
        """
        return self._passes

    @passes.setter
    def passes(self, new_passes_list):
        assert isinstance(new_passes_list, list)
        self._passes = list(new_passes_list)


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
    def existing_piletter(self):
        """Returns if a piletter was already available in the experiment folder
        before starting this post-process.
        """
        return self._existing_piletter

    @existing_piletter.setter
    def existing_piletter(self, status):
        self._existing_piletter = status

    @property
    def existing_lisfile(self):
        """Returns if the lis file(s) were already available in the experiment folder
        before starting this post-process.
        """
        return self._existing_lisfiles

    @existing_lisfile.setter
    def existing_lisfile(self, status):
        self._existing_lisfiles = status

    # @property
    # def operations(self):
    #     """Dictionary with parameters that have been used during the post-process.
    #     They key will may refer to the action/program that has been used and the value
    #     the parameter that has been used to trigger it.
    #     For example,  'flag_weights.py': 0.9  or 'polswap.py': 'Ef,Tr' will make reference
    #     to the weight threshold established when running flag_weights.py, and the two
    #     stations that exhibited swap pols.
    #     """
    #     return self._operations
    #
    # @property
    # def operations(self, new_dict):
    #     """A new dictionary with pair(s) key/value to be added to the current operations
    #     is expected.
    #     """
    #     for a_new_key in new_dict:
    #         self._operations[a_new_key] = new_dict[a_new_key]


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
        self._supsci = support_scientist
        self._obsdate = None
        self._processdate = dt.datetime.now()
        # Attributes not known until the MS file is created
        self._startime = None
        self._endtime = None
        self._antennas = ()
        self._ref_antennas = ()
        self._onebit_antennas = ()
        self._polconvert_antennas = ()
        self._sources = None
        self._ref_sources = None
        self._credentials = Credentials(None, None)
        self._number_passes = None
        self._passes = []


    def get_setup_from_ms(self):
        """Obtains the time range, antennas, sources, and frequencies of the observation
        from all existing passes with MS files and incorporate them into the current object.
        """
        for a_pass in self.passes:
            try:
                with pt.table(a_pass.msfile, readonly=True, ack=False) as ms:
                    with pt.table(ms.getkeyword('ANTENNA'), readonly=True, ack=False) as ms_ant:
                        self.antennas = [ant.upper() for ant in ms_ant.getcol('NAME')]

                    with pt.table(ms.getkeyword('FIELD'), readonly=True, ack=False) as ms_field:
                        a_pass.sources = ms_field.getcol('NAME')

                    with pt.table(ms.getkeyword('OBSERVATION'), readonly=True, ack=False) \
                                                                                 as ms_obs:
                        self.timerange = dt.datetime(1858, 11, 17, 0, 0, 2) + \
                             ms_obs.getcol('TIME_RANGE')[0]*dt.timedelta(seconds=1)
                    with pt.table(ms.getkeyword('SPECTRAL_WINDOW'), readonly=True, ack=False) \
                                                                                    as ms_spw:
                        a_pass.freqsetup(ms_spw.getcol('NUM_CHAN'), ms_spw.getcol('CHAN_FREQ'),
                                         ms_spw.getcol('TOTAL_BANDWIDTH'))
            except RuntimeError:
                print(f"WARNING: {a_pass.msfile} not found.")

        # NOTE: Get also the frequency (subband) information.


    def parse_expsum(self):
        """Parses the .expsum file associated to the experiment to get different
        valuable data as:
            PI/contact-author name(s) and emails.
            Number of correlator passes.
            Sources to be observed.
        """
        if not os.path.isfile(f"{self.expname.lower()}.expsum"):
            raise FileNotFoundError(f"ERROR: {self.expname.lower()}.expsum file not found.")

        with open(f"{self.expname.lower()}.expsum", 'r') as expsum:
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








