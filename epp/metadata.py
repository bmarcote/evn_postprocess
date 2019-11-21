"""Defines an EVN experiment with all the relevant metadata
required during the post-processing.

The metadata is obtained from different sources and/or at
different stages of the post-processing.
"""
import subprocess
import datetime as dt
from pyrap import tables as pt


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
        assert (isinstance(username, str) & isinstance(password, str))
        self._username = username
        self._password = password



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
        """Returns the name of the MS file associated for this correlator pass.
        """
        return self._msfile

    @property
    def sources(self):
        """List of sources present in this correlator pass.
        """
        return self._sources

    @sources.setter
    def sources(self, list_of_sources):
        self._sources = list_of_sources


    def __init__(self, lisfile, msfile):
        self._lisfile = lisfile
        self._msfile = msfile
        self._sources = []
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
    def obsdate(self):
        """Epoch at which the EVN experiment was observed (starting date), in YYMMDD format.
        """
        return self._obsdate

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
    def timerange(self, starttime, endtime):
        """Start and end time of the observation in datetime format.
        """
        assert isinstance(starttime, datetime)
        assert isinstance(endtime, datetime)
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
    def sources(self):
        """List of sources observed in the experiment.
        """
        return self._sources

    @sources.setter
    def sources(self, new_sources):
        """List of sources observed in the experiment.
        """
        self._sources = tuple(new_sources)

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
        self._passes = new_passes_list


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


    def __init__(self, expname, **kwargs):
        """Initializes an EVN experiment with the given name.

        Inputs:
        - expname : str
               The name of the experiment (case insensitive).
        """
        self._expname = expname.upper()
        self._piname = None
        self._email = None
        self._obsdate = self.get_obsdate_from_ccs()
        # Attributes not known until the MS file is created
        self._startime = None
        self._endtime = None
        self._antennas = []
        self._credentials = Credentials(None, None)
        self._passes = []


    def get_obsdate_from_ccs(self):
        """Obtains the observing epoch from the MASTER_PROJECTS.LIS located in ccc.
        In case of being an e-EVN experiment, it will add that information to self.eEVN.
        """
        cmd = f"grep {self.expname} /ccs/var/log2vex/MASTER_PROJECTS.LIS"
        process = subprocess.Popen(["ssh", "jops@ccs", cmd], shell=False, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        output = process.communicate()[0].decode('utf-8')
        if process.returncode != 0:
            raise ValueError(f"Error code {process.returncode} when reading MASTER_PROJECTS.LIS from ccs.")

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
                    self._eEVN = an_input[0]

            return obsdate[2:]

        elif output.count('\n') == 1:
            self._eEVN = None
            return output[:-1].split()[1].strip()[2:]

        else:
            raise ValueError(f"{self.expname} not found in (ccs) MASTER_PROJECTS.LIS or connection not set.")


    def get_setup_from_ms(self):
        """Obtains the time range, antennas, sources, and frequencies of the observation
        from all existing passes with MS files and incorporate them into the current object.
        """
        for a_pass in self.passes:
            with pt.table(a_pass.msfile, readonly=True, ack=False) as ms:
                with pt.table(ms.getkeyword('ANTENNA'), readonly=True, ack=False) as ms_ant:
                    self.antennas = [ant.upper() for ant in ms_ant.getcol('NAME')]

                with pt.table(ms.getkeyword('FIELD'), readonly=True, ack=False) as ms_field:
                    self.sources = ms_field.getcol('NAME')

                with pt.table(ms.getkeyword('OBSERVATION'), readonly=True, ack=False) as ms_obs:
                    # TODO: check this. It is wrong
                    self.starttime = dt.datetime(1858, 11, 17, 0, 0, 2) + \
                                         ms_obs.getcol('TIME_RANGE')[0]*dt.timedelta(seconds=1)

        # NOTE: Get also the frequency (subband) information.



