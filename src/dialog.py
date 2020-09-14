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
import curses
from textwrap import wrap
import configparser
import logging
import subprocess
from enum import Enum
from datetime import datetime
import numpy as np
from . import metadata
from . import actions
from . import process_eee as eee

import npyscreen as nps


class Choice(Enum):
    ok = 0
    abort = 1
    repeat = 2


class PopUp(nps.utilNotify.ConfirmCancelPopup):
    def __init__(self, ok_label, cancel_label, *args, **kwargs):
        self.OK_BUTTON_TEXT = ok_label
        self.CANCEL_BUTTON_TEXT = cancel_label
        super().__init__(*args, **kwargs)


def notify_popup(message, title="Message", form_color='STANDOUT', wrap=True, editw=0, ok_label='OK',
                 cancel_label='Cancel'):
    message = nps.utilNotify._prepare_message(message)
    curses.initscr()
    F = PopUp(ok_label=ok_label, cancel_label=cancel_label, name=title, color=form_color)
    F.preserve_selected_widget = True
    mlw = F.add(nps.wgmultiline.Pager,)
    mlw_width = mlw.width - 1
    if wrap:
        message = nps.utilNotify._wrap_message_lines(message, mlw_width)

    mlw.values = message
    F.editw = editw
    F.edit()
    return F.value


class FirstForm(nps.ActionFormV2):
    OK_BUTTON_TEXT = "Continue"
    CANCEL_BUTTON_TEXT = "Abort"
    CANCEL_BUTTON_BR_OFFSET = (2, 17)

    def __init__(self, exp, *args, **kwargs):
        self.exp = exp
        super().__init__(*args, **kwargs)

    def create(self):
        y, x = self.useable_space()
        all_ants = ', '.join([s.capitalize() for s in self.exp.antennas])
        self.add(nps.BoxTitle, name="Scheduled antennas", max_width=(x // 2) - 3,
                 values=wrap(all_ants, (x // 2) - 5), editable=False, max_height=6)
        self.ref_ant = self.add(nps.TitleMultiSelect, name="Reference antenna(s):", max_width=x // 2, max_height=6,
                                values=self.exp.antennas, value=[i for i in range(len(self.exp.antennas)) \
                                if self.exp.antennas[i] in self.exp.ref_antennas], scroll_exit=True)
        self.onebit = self.add(nps.TitleMultiSelect, name="1-bit antenna(s)?:", max_width=x // 2, max_height=4,
                               values=self.exp.antennas, value=[], scroll_exit=True)
        self.cal_sources = self.add(nps.TitleMultiSelect, name='Sources for Plotting:',
            values=[s.name for s in self.exp.sources], scroll_exit=True, max_height=6, max_width=x // 2,
            value=np.arange(len(self.exp.sources))[[True if s.type == metadata.SourceType.fringefinder else False for s in self.exp.sources]])
        # self.nextrely += 1  # To get more space between widgets
        self.passes = self.add(nps.TitleMultiSelect, name='Passes to pipeline', max_width=x // 2, max_height=4,
            values=[p.lisfile.replace(self.exp.expname.lower(), '').replace('.lis', '') if p.lisfile != f"{self.exp.expname.lower()}.lis" else p.lisfile for p in self.exp.passes],
            value=np.arange(len(self.exp.passes))[[p.pipeline for p in self.exp.passes]])

        # You can use the negative coordinates
        # self.add(nps.TitleFilename, name="Filename:", rely=-5)
        ## Build the output messages
        msg = []
        if 'checklis' in self.exp.stored_outputs:
            msg += ['> checklis', *self.exp.stored_outputs['checklis'].split('\n')]
            if len(self.exp.stored_outputs['checklis'].split('\n')) > 2:
                msg += ['CORRECT LIS FILE BEFORE CONTINUE IF NEEDED.']

        if actions.station_1bit_in_vix(f"{self.exp.expname.upper()}.vix"):
            msg += ['WARNING: There may be a 1-bit station.']

        missing_lisfiles = []
        for a_pass in self.exp.passes:
            if not os.path.isfile(a_pass.lisfile):
                all_lisfiles.append(a_pass.lisfile)

        if len(missing_lisfiles) > 0:
            msg += ['ERROR: Expecting but not found:', *wrap(', '.join(all_lisfiles), (x // 2) - 8)]

        if self.exp.credentials.password is None:
            msg += ['NOTE: No authentification will be set for this experiment.']
        else:
            msg += ['Authentification:', f"username: {self.exp.credentials.username}",
                    f"password: {self.exp.credentials.password}"]

        self.add(nps.BoxTitle, name="Log Messages", relx=(x // 2) + 1, rely=2, max_width=(x // 2) - 6, max_height=y-6,
                 values=msg, editable=False)


    def afterEditing(self):
        self.parentApp.setNextForm(None)
        self.parentApp.switchForm(None)

    def on_ok(self):
        self.parentApp.setNextForm(None)
        self.parentApp.switchForm(None)
        self.exp.ref_antennas = (self.exp.antennas[i] for i in self.ref_ant.value)
        self.exp.ref_sources = (self.exp.sources[int(i)].name for i in self.cal_sources.value)
        self.exp.onebit_antennas = (self.exp.antennas[i] for i in self.onebit.value)


    def on_cancel(self):
        print('Stopping processing...')
        self.parentApp.switchFormPrevious()
        sys.exit(0)

    # def exit_application(self):
    #     self.parentApp.switchForm(None)
    #     self.editing = False


class DialogManager(nps.NPSAppManaged):
    def __init__(self, exp, form, *args, **kwargs):
        self.exp = exp
        self.form = form
        super().__init__(*args, **kwargs)

    def onStart(self):
        self.BLANK_LINES_BASE = 0
        self.addForm('MAIN', self.form, self.exp,
             name=f"Processing of {self.exp.expname.upper()} --- {self.exp.obsdate} --- "
                  f"{self.exp.obsdatetime.strftime('%d %b %Y (%j)')}")



def first_dialog(exp):
    """Loads the FirstDialogForm
    """
    # TODO: If reference antenna and reference source are given, and checklis run OK, no need to ask.
    DialogManager(exp, FirstForm).run()
    # THIS IS THE MANUALLY RUNNING WITHOUT NPS TO CHECK EVERYTHING WORKS
    # exp.stored_outputs['checklis'] has the output
    # NOTE: if refant, calsour are defined, and checklis output message only has two lines... then no need for this!!
    # all_ants = ', '.join([s.capitalize() for s in exp.antennas])
    # print('Scheduled antennas: ' + all_ants + '.')
    # ref_ant = actions.ask_user('Insert the ref. antenna', accepted_values=exp.antennas)
    # onebit_ant = actions.ask_user('Any 1-bit antenna? (enter to skip)')
    # print(f"Inserted refant ({ref_ant}) and 1-bit ant ({onebit_ant}).")
    # exp.ref_antennas = ref_ant
    # if onebit_ant not in ('', None):
    #     exp.onebit_antennas = onebit_ant.split(',')
    #
    # print(f"Scheduled sources: {', '.join([s.name for s in exp.sources])}.")
    # calsour = actions.ask_user('Pick the ref sources for standard plots (comma-sep)')
    # exp.ref_sources = calsour.split(',')
    # # Passes for pipeline




class RedoPlotsForm(nps.ActionFormV2):
    OK_BUTTON_TEXT = "Continue"
    CANCEL_BUTTON_TEXT = "Abort"
    CANCEL_BUTTON_BR_OFFSET = (2, 17)

    def __init__(self, exp, *args, **kwargs):
        self.exp = exp
        super().__init__(*args, **kwargs)

    def create(self):
        y, x = self.useable_space()
        all_ants = ', '.join([s.capitalize() for s in self.exp.antennas])
        self.add(nps.BoxTitle, name="Run again standardplots", max_width=(x // 2) - 3,
                 values=wrap("You can modify here the selection to re-run standardplots.", (x // 2) - 5),
                 editable=False, max_height=5)
        self.ref_ant = self.add(nps.TitleMultiSelect, name="Reference antenna(s):", max_width=x // 2, max_height=y - 8,
                                values=self.exp.antennas, value=[i for i in range(len(self.exp.antennas)) \
                                if self.exp.antennas[i] in self.exp.ref_antennas], scroll_exit=True)
        self.cal_sources = self.add(nps.TitleMultiSelect, name='Sources for Plotting:', relx=(x // 2) + 1,
            values=[s.name for s in self.exp.sources], scroll_exit=True, max_height=y - 8, max_width=(x // 2) - 6,
            value=[i for i in range(len(self.exp.sources)) if self.exp.sources[i] in self.exp.ref_sources])


    def afterEditing(self):
        self.parentApp.setNextForm(None)
        self.parentApp.switchForm(None)

    def on_ok(self):
        self.parentApp.setNextForm(None)
        self.parentApp.switchForm(None)
        self.exp.ref_antennas = (self.exp.antennas[i] for i in self.ref_ant.value)
        self.exp.ref_sources = (self.exp.sources[int(i)].name for i in self.cal_sources.value)

    def on_cancel(self):
        print('Stopping processing...')
        self.parentApp.switchFormPrevious()
        sys.exit(0)

    # def exit_application(self):
    #     self.parentApp.switchForm(None)
    #     self.editing = False


class AfterPlotsForm(nps.ActionFormV2):
    OK_BUTTON_TEXT = "Continue"
    CANCEL_BUTTON_TEXT = "Abort"
    CANCEL_BUTTON_BR_OFFSET = (2, 17)

    def __init__(self, exp, *args, **kwargs):
        self.exp = exp
        super().__init__(*args, **kwargs)

    def create(self):
        y, x = self.useable_space()
        all_ants = ', '.join([s.capitalize() for s in self.exp.antennas])
        if 'standardplots' in self.exp.stored_outputs:
            msg_setup = self.exp.stored_outputs['standardplots']
        else:
            msg_setup = "Standardplots did not run in this iteraction. " \
                        "Please check manually the recorded outputs" \
                        " to know which antennas have data."

        self.add(nps.BoxTitle, name=f"{self.exp.expname} setup:", max_height=8, editable=False,
                 values=wrap(msg_setup, x - 8))
        # TODO: add a widget with no data from the following antennas:
        self.add(nps.BoxTitle, name="MS operations", max_height=5, editable=False,
                 values=wrap("Select in case of changes are required to apply to the dataset.", x - 8))
        self.flagweights = self.add(nps.Slider, name="Flag weights",
                               out_of=1.0, step=0.05, lowest=0.0, value=0.9)
        self.polswap = self.add(nps.TitleMultiSelect, name="Swapping pols:", max_height=5,
                               max_width=(x // 2), values=self.exp.antennas, value=[], scroll_exit=True)
        self.polconv = self.add(nps.TitleMultiSelect, name="Pol Convert:", max_height=5,
                               relx=(x // 2) + 1, max_width=(x // 2) - 6,
                               values=self.exp.antennas, value=[], scroll_exit=True)

    def afterEditing(self):
        self.parentApp.setNextForm(None)
        self.parentApp.switchForm(None)

    def on_ok(self):
        self.parentApp.setNextForm(None)
        self.parentApp.switchForm(None)
        self.exp.flagged_weights = metadata.FlagWeight(self.flagweights.value)
        self.exp.polswap_antennas = (self.exp.antennas[i] for i in self.polswap.value)
        self.exp.polconv_antennas = (self.exp.antennas[i] for i in self.polconv.value)

    def on_cancel(self):
        print('Stopping processing...')
        self.parentApp.switchFormPrevious()
        sys.exit(0)

    def on_rerun(self):
        #TODO
        pass

    # def exit_application(self):
    #     self.parentApp.switchForm(None)
    #     self.editing = False



def redoplots_dialog(exp):
    """Loads the Dialog to be able to repeat standardplots.
    """
    # TODO: If reference antenna and reference source are given, and checklis run OK, no need to ask.
    DialogManager(exp, RedoPlotsForm).run()


def afterplots_dialog(exp):
    """Loads the Dialog to perform the MS operations after standardplots
    """
    # TODO: If reference antenna and reference source are given, and checklis run OK, no need to ask.
    DialogManager(exp, AfterPlotsForm).run()




def standardplots_dialog(exp):
    """To be run right after standardplots.
    It will ask the user if all plots look OK and the post-processing
    can continue (in that case it asks the weight threshold to set for
    flagging, and if any station requires polarization corrections).
    Otherwise it allows to re-run standardplots with an updated list
    of ref-antennas and cal. sources.
    """
    message = "Do you want to repeat standardplots or continue with the post-processing?"
    while not notify_popup(message, title=f"{exp.expname} -- Question", form_color='STANDOUT', wrap=True,
                                                   editw=0, ok_label='Continue', cancel_label='Repeat'):
        redoplots_diaog(exp)
        eee.standardplots(exp)
        open_standardplot_files(exp)



def warning_dialog(message, title):
    """Produces a Warning dialog showing some text.
    Allows two options: Stop (to stop the full program),
    or Continue (to ignore the warning and keep it running).

    Returns a bool with the choice.
    """
    nps.notify_confirm(message, title=title)


def continue_dialog(message, title):
    if not notify_popup(message, title=title, form_color='STANDOUT', wrap=True,
                    editw=0, ok_label='Continue', cancel_label='Abort'):
        sys.exit(0)




