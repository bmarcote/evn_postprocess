"""Defines an EVN experiment with all the relevant metadata
required during the post-processing.

The metadata is obtained from different sources and/or at
different stages of the post-processing.
"""




class Experiment():
    """Defines and EVN experiment with all relevant metadata.
    """

    @property
    def expname(self):
        """Name of the EVN experiment, in upper cases.
        """
        return self._expname

    @property
    def eEVN(self):
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


    def __init__(self, expname, **kwargs):
        """Initializes an EVN experiment with the given name.

        Inputs:
        - expname : str
               The name of the experiment (case insensitive).
        """
        self._expname = expname.upper()
        self._obsdate = self.get_obsdate_from_ccs()


    def get_obsdate_from_ccs(self):
        """Obtains the observing epoch from the MASTER_PROJECTS.LIS located in ccc.
        In case of being an e-EVN experiment, it will add that information to self.eEVN.
        """
        cmd = "grep {} /ccs/var/log2vex/MASTER_PROJECTS.LIS".format(self.expname)
        process = subprocess.Popen(["ssh", "jops@ccs", cmd], shell=False, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        output = process.communicate()[0].decode('utf-8')
        if ssh.returncode != 0:
            raise ValueError('Error code {} when reading MASTER_PROJECTS.LIS from ccs.'.format(ssh.returncode))

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

            return obsdate

        elif output.count('\n') == 1:
            self._eEVN = None
            return output[:-1].split('\n')[1].strip()

        else:
            raise ValueError('{} not found in (ccs) MASTER_PROJECTS.LIS or connection not setted.'.format(self.expname))




