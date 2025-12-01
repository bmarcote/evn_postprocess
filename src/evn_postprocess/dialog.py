import abc
import sys
from . import experiment
from . import utils 

class Dialog(object, metaclass=abc.ABCMeta):
    """Abstract class that implements the basic functionality for any
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

        These parameters need to be loaded into the respective parameters inside the exp
        object (passed to the function).
        
        Args:
            exp (experiment.Experiment): Experiment object to update with user-provided parameters.
        
        Returns:
            bool: True if the dialog and recording of the parameters went successfully.
        """
        raise NotImplementedError('users must define this function to use this base class')


class Terminal(Dialog):

    def ask_for_antennas(self, exp, asking_text):
        """Asks for a list of antennas and parses them.
        It verifies that all introduced antennas are included in the experiment.
        
        Args:
            exp (experiment.Experiment): Experiment object containing valid antenna names.
            asking_text (str): Text prompt to display to the user.
        
        Returns:
            list[str]: List of antenna names provided by the user, or empty list if none specified.
        """
        antennas = []
        while True:
            try:
                output = input(asking_text).replace('\n', '')
                if output != '':
                    antennas = [ant.strip().capitalize() for ant in \
                                output.split(',' if ',' in output else ' ')]
                    for antenna in antennas:
                        if antenna not in exp.antennas.names:
                            raise ValueError(f"Antenna {antenna} not recognized (not included "
                                             f"in {', '.join(exp.antennas.names)})")
                break
            except ValueError as e:
                print(f'ValueError: {e}.')
                continue
            except KeyboardInterrupt:
                print('\nPipeline aborted !')
                sys.exit(1)

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
        
        Args:
            exp (experiment.Experiment): Experiment object to update with user-provided parameters.
        
        Returns:
            bool: True if the dialog and recording of the parameters went successfully.
        """
        print("\n\n\n### Please answer to the following questions:\n")
        while True:
            try:
                threshold = float(input("\n\033[1mThreshold for flagging weights "
                                        "in the MS:\n>\033[0m "))
                if 0.0 < threshold < 1.0:
                    break
                else:
                    print("The threshold needs to be a value within [0.0, 1.0).")

            except ValueError:
                print('ValueError: could not convert input to float (for threshold).')
                continue

        polswap = self.ask_for_antennas(exp, "\n\033[1mAntennas for polswap (comma or " \
                                        "Fspace separated)\n\033[0m(possible antennas are: "
                                             f"{', '.join(exp.antennas.names)})\n\033[1m>\033[0m ")
        if utils.station_1bit_in_vix(exp.vix):
            onebit = self.ask_for_antennas(exp, "\n\033[1mAntennas that recorded one-bit " \
                                                "data:\n> \033[0m")
        else:
            onebit = []

        polconvert = self.ask_for_antennas(exp, "\n\033[1mAntennas that requires PolConvert" \
                                                ":\n> \033[0m")

        for i in range(len(exp.correlator_passes)):
            exp.correlator_passes[i].flagged_weights = experiment.FlagWeight(threshold)

        for antenna in polswap:
            exp.antennas[antenna].polswap = True

        for antenna in polconvert:
            exp.antennas[antenna].polconvert = True

        for antenna in onebit:
            exp.antennas[antenna].onebit = True

        return True








