"""Defines an EVN experiment with all the relevant metadata
required during the post-processing.

The metadata is obtained from different sources and/or at
different stages of the post-processing.
"""
import subprocess
from datetime import datetime
from pyrap import tables as pt




class Experiment():
    """Defines and EVN experiment with all relevant metadata.
    """
    class Credentials():
        def __init__(self, username, password):
            self.username = username
            self.password = password

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
    def obsdate(self):
        """Epoch at which the EVN experiment was observed (starting date), in YYMMDD format.
        """
        return self._obsdate

    @property
    def obsdatetime(self):
        """Epoch at which the EVN experiment was observed (starting date), in datetime format.
        """
        return datetime.strptime(self.obsdate, '%y%m%d')

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
    def credentials(self):
        """Username and password to access the experiment data from the EVN
        archive during the proprietary period.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, username, password):
        self._credentials = Credentials(username, password)


    def __init__(self, expname, **kwargs):
        """Initializes an EVN experiment with the given name.

        Inputs:
        - expname : str
               The name of the experiment (case insensitive).
        """
        self._expname = expname.upper()
        self._obsdate = self.get_obsdate_from_ccs()
        # Attributes not known until the MS file is created
        self._startime = None
        self._endtime = None
        self._antennas = []
        self._sources = []
        self._credentials = self.Credentials(None, None)


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
            inputs = [i.split().strip() for i in output[:-1].split('\n')]
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


    def get_setup_from_ms(self, msfile):
        """Obtains the time range, antennas, sources, and frequencies of the observation
        from the specified MS file and incorporate them into the current object.
        """
        # NOTE: Do this, recycle from the casa gmrt/evn pipeline.
        # NOTE: Antennas should be converted to upper cases to make everything easier
        with pt.table(msfile, readonly=True, ack=False) as ms:
            with pt.table(ms.getkeyword('ANTENNA'), readonly=True, ack=False) as ms_ant:
                self._antennas = [ant.upper() for ant in ms_ant.getcol('NAME')]

            with pt.table(ms.getkeyword('FIELD'), readonly=True, ack=False) as ms_field:
                self._sources = ms_field.getcol('NAME')

            with pt.table(ms.getkeyword('OBSERVATION'), readonly=True, ack=False) as ms_obs:
                min_time, max_time



