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
    def sources(self):
        """List of sources present in this correlator pass.
        """
        return self._sources

    def __init__(self):
        self._sources = []



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

    @property
    def email(self):
        return self._email

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

    @property
    def antennas(self):
        """List of antennas available in the experiment.
        """
        return self._antennas


    @property
    def sources(self):
        """List of sources observed in the experiment.
        """
        return self._sources


    @property
    def passes(self):
        """List of all correlator passes (one or more) that have been conducted.
        Each element of the list is a CorrelatorPass object with all the relevant
        associated information that may vary for each pass.
        The order of the elements is relevant as the first one is considered the
        reference pass (e.g. the one to produce the *_1_1.IDI files).
        """
        return self._passes


    @setter.passes
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


    def get_pi_from_expsum(self):
        """Obtains the PI name and the email from the .expsum file that is expected to be
        placed in the current directory. Adds this information to the object.
        """
        try:
            with open(f"{self.expname.lower()}.expsum", 'r') as expsumfile:
                for a_line in expsumfile.readlines():
                    if 'Principal Investigator:' in a_line:
                        # The line is expected to be '  Principal Investigator: SURNAME  (EMAIL)'
                        piname, email = a_line.split(':')[1].split('(')
                        self._piname = piname.strip()
                        self._email = email.replace(')','').strip()
        except FileNotFoundError as e:
            raise e(f"ERROR: {self.expname.lower()}.expsum is not found.")


    def print_sourcelist_from_expsum(self):
        """Prints the source list as appears in the .expsum file for the given experiment.
        """
        try:
            with open(f"{self.expname.lower()}.expsum", 'r') as expsumfile:
                sourcelist = []
                for a_line in expsumfile.readlines():
                    if 'src =' in a_line:
                        sourcelist.append(a_line)

                print('\nSource list:')
                print('\n'.join(sourcelist))
        except FileNotFoundError as e:
            raise e(f"ERROR: {self.expname.lower()}.expsum is not found.")


    def get_setup_from_ms(self, msfile):
        """Obtains the time range, antennas, sources, and frequencies of the observation
        from the specified MS file and incorporate them into the current object.
        """
        with pt.table(msfile, readonly=True, ack=False) as ms:
            with pt.table(ms.getkeyword('ANTENNA'), readonly=True, ack=False) as ms_ant:
                self._antennas = [ant.upper() for ant in ms_ant.getcol('NAME')]

            with pt.table(ms.getkeyword('FIELD'), readonly=True, ack=False) as ms_field:
                self._sources = ms_field.getcol('NAME')

            with pt.table(ms.getkeyword('OBSERVATION'), readonly=True, ack=False) as ms_obs:
                self._starttime, self._endtime = dt.datetime(1858, 11, 17, 0, 0, 2) + \
                                     ms_obs.getcol('TIME_RANGE')[0]*dt.timedelta(seconds=1)

        # NOTE: Get also the frequency (subband) information.



