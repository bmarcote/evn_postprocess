import abc
from . import experiment



class Dialog(object, metaclass=abc.ABCMeta):
    """Abstract class that implements the basic functionallity for any
    User Interface required for the post-processing.
    """
    @abc.abstractmethod
    def askMSoperations(self, exp):
        """Dialog that requests the following parameters in order to process the MS
        of a given experiment:

        - Weight threshold for the flagging in the MS. A float number between 0 and 1.0.
        - Antennas that require a polswap.
        - Antennas that recorded one-bit data and require the conversion to two-bit.
        - Antennas that require to run PolConvert because they recorded linear polarization.

        These parameters needs to be loaded into the respective parameters inside the exp
        object (passed to the function).

        It should return a bool indicating if the dialog and recording of the parameters
        went sucessfully.
        """
        raise NotImplementedError('users must define this function to use this base class')





class Terminal(Dialog):

    def ask_for_antennas(self, exp, asking_text):
        """Asks for a list of antennas and parses them.
        It returns None if none are specified or a Python list of the introduced antennas.
        It verifies that all introduced antennas are included in the experiment.
        """
        antennas = None
        while True:
            try:
                polswap = input(f'{asking_text}:\n').replace('\n', '')
                if polswap != '':
                    antennas = [ant.strip().capitalize() for ant in polswap.split(',' if ',' in polswap else ' ')]
                    for antenna in antennas:
                        if antenna not in exp.antennas.names:
                            raise ValueError(f"Antenna {antenna} not recognized (not included " \
                                             f"in {', '.join(exp.antennas.names)})")
                    break
            except ValueError as e:
                print(f'ValueError: {e}.')
                continue

        return antennas


    def askMSoperations(self, exp):
        """Dialog that requests the following parameters in order to process the MS
        of a given experiment:

        - Weight threshold for the flagging in the MS. A float number between 0 and 1.0.
        - Antennas that require a polswap.
        - Antennas that recorded one-bit data and require the conversion to two-bit.
        - Antennas that require to run PolConvert because they recorded linear polarization.

        These parameters are loaded into the respective parameters inside the exp
        object (passed to the function).

        It returns a bool indicating if the dialog and recording of the parameters
        went sucessfully.
        """
        while True:
            try:
                threshold = float(input('Threshold for flagging weights in the MS:\n'))
                if 0.0 < threshold < 1.0:
                    break
                else:
                    print(f"The threshold needs to be a value within [0.0, 1.0).")

            except ValueError as e:
                print(f'ValueError: could not convert string to float: {threshold}')
                continue

        polswap = self.ask_for_antenna('Antennas for polswap (comma or space separated)')
        onebit = self.ask_for_antenna('Antennas that recorded one-bit data')
        polconvert = self.ask_for_antenna('Antennas that requires PolConvert')

        for a_pass in exp.correlator_passes:
            exp.correlator_passes[a_pass].flagged_weights = experiment.FlagWeight(threshold)

        for antenna in polswap:
            exp.antennas[antenna].polswap = True

        for antenna in polconvert:
            exp.antennas[antenna].polconvert = True

        for antenna in onebit:
            exp.antennas[antenna].onebit = True

        return True








