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
from textwrap import wrap
import configparser
import logging
import subprocess
from enum import Enum
from datetime import datetime
from . import metadata
from . import actions
from . import process_eee as eee

import npyscreen as nps


class Choice(Enum):
    ok = 0
    abort = 1
    repeat = 2



class FirstForm(nps.ActionForm):
    def __init__(self, exp, *args, **kwargs):
        self.exp = exp
        super().__init__(*args, **kwargs)

    def create(self):
        x,y = self.useable_space()
        # print(x)
        all_ants = ', '.join([s.capitalize() for s in self.exp.antennas])
        self.add(nps.BoxTitle, name="Scheduled antennas",
                 values=wrap(all_ants, 40), editable=False, max_height=6, max_width=40)
                 # value=', '.join([s.capitalize() for s in self.exp.antennas]), editable=False)
        self.ref_ant = self.add(nps.TitleText, name="Ref. ant:", value='(ant code)')
        # self.nextrely += 1  # To get more space between widgets
        self.onebit = self.add(nps.TitleText, name="1-bit ant?:", value='(ant code)?')
        # sources = set()
        # type_sour = {metadata.SourceType.target: 't', metadata.SourceType.calibrator: 'c',
        #              metadata.SourceType.fringefinder: 'ff', metadata.SourceType.other: 'o'}
        # for a_pass in self.exp.passes:
        #     for a_sour in a_pass.sources:
        #         sources.add(f"{a_sour.name} ({type_sour[a_sour.type]})")
        self.cal_sources = self.add(nps.TitleMultiSelect, name='Cals. to plot',
            values=[s.name for s in self.exp.sources])
            # value=arange(len(self.exp.sources))[True if s.type == metadata.SourceType.fringefinder else False for s in self.exp.sources], max_width=x//2))
        self.passes = self.add(nps.TitleMultiSelect, name='Passes to pipeline', values=[p.lisfile.replace(self.exp.expname.lower(), '').replace('.lis', '') for p in self.exp.passes], value=arange(len(self.exp.passes))[p.pipeline for p in self.exp.passes])

        # self.add(nps.BoxTitle, name="Output", relx=x//2, max_width=x//2, values="Text...", editable=False)
        # self.add()
        # self.nextrely += 1  # To get more space between widgets
        # self.add()

    def afterEditing(self):
        self.parentApp.setNextForm(None)


class FirstDialog(nps.NPSAppManaged):
    def __init__(self, exp, *args, **kwargs):
        self.exp = exp
        super().__init__(*args, **kwargs)

    def onStart(self):
        self.addForm('MAIN', FirstForm, self.exp,
             name=f"Process of {self.exp.expname.upper()} --- {self.exp.obsdate} --- "
                  f"{self.exp.obsdatetime.strftime('%d %b %Y (%j)')}")



def first_dialog(exp):
    """Loads the FirstDialogForm
    """
    # Needs to run checklis
    # Also check that there are enough lis files.
    # And if onebit antenna
    first_form = FirstDialog(exp).run()




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


