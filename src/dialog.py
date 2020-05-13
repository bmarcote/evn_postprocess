#!/usr/bin/env python3
"""Script that runs interactive SFXC post-correlation steps.
This module produces all the dialogs that are required
during the running.
"""

import os
import sys
import glob
import string
import random
import argparse
import configparser
import logging
import subprocess
from datetime import datetime
from src import metadata
from src import actions

import npyscreen as nps


class Choice(Enum):
    ok = 0
    abort = 1
    repeat = 2


def first_dialog(exp):
    """Loads the FirstDialogForm
    """
    pass


def warning_dialog(exp, textbody):
    """Produces a Warning dialog showing some text.
    Allows two options: Stop (to stop the full program),
    or Continue (to ignore the warning and keep it running).

    Returns a bool with the choice.
    """
    pass



def standardplots_dialog(exp):
    """To be run right after standardplots.
    It will ask the user if all plots look OK and the post-processing
    can continue (in that case it asks the weight threshold to set for
    flagging, and if any station requires polarization corrections).
    Otherwise it allows to re-run standardplots with an updated list
    of ref-antennas and cal. sources.
    """
    pass
    # return {'choice': choice, 'polswap': ant_list, 'polconvert': ant_list,
    #         'flagweight': threshold, 'ref_ant': ref_ant_list, 'cal_sources': calsour_list}
    # If optiones were not used, then the value is None


class FirstDialogForm(nps.Form):
    """After loading all files belonging to the experiment exp,
    its asks to confirm that the .lis files are OK and asks for
    the calibrators and correlator passes to pipeline.
    """
    def create(self):
        pass


