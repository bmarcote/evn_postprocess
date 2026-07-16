#!/usr/bin/env python3
"""Appends the Tsys and GC information from the ANTAB file into the FITS-IDI files

Version: 1.5
Date: Apr 2022
Written by Benito Marcote (marcote@jive.eu)
"""
import os
import glob
from natsort import natsort_keygen
from pathlib import Path
import argparse
import subprocess

__version__ = '1.5'



def main(antabfile, idifiles, replace=False):
    """Runs append_tsys.py and append_gc.py to incorporate the Tsys and the GC
    information from the ANTAB file into all FITS-IDI files.
    """
    if not os.path.isfile(antabfile):
        raise FileNotFoundError(f"The ANTAB file {antabfile} cannot be found.")

    def parse(s):
        # Because I do not know regrex
        i0 = s.index('_')
        return int(s[i0 + 1:(i0 + 1 + s[i0+1:].index('_'))])

    # In case there are different passes but a single antab file
    for a_phase_center in set([parse(an_idi) for an_idi in idifiles]):
        these_idi_files = [idi for idi in idifiles if parse(idi) == a_phase_center]
        print(f"Running append_tsys.py {antabfile} {' '.join(these_idi_files)}...")
        if replace:
            proc = subprocess.Popen(["append_tsys.py", '--replace', antabfile, *these_idi_files],
                                    stdout=None, stderr=subprocess.STDOUT)
        else:
            proc = subprocess.Popen(["append_tsys.py", antabfile, *these_idi_files], stdout=None,
                                    stderr=subprocess.STDOUT)

        while proc.poll() is None:
            pass

    for idifile in [idi for idi in idifiles if (idi.endswith('.IDI1') or idi.endswith('IDI'))]:
        print(f"Running append_gc.py {antabfile} {idifile}...")
        if replace:
            proc = subprocess.Popen(["append_gc.py", '--replace', antabfile, idifile],
                                    stdout=None, stderr=subprocess.STDOUT)
        else:
            proc = subprocess.Popen(["append_gc.py", antabfile, idifile], stdout=None,
                                    stderr=subprocess.STDOUT)
        while proc.poll() is None:
            pass


if __name__ == '__main__':
    help_antabfile = "ANTAB file to be read."
    help_fitsidifiles = "FITS-IDI files to incorporate the ANTAB (Tsys and GC) information. " \
                        "You can use wildcards but then the string MUST be enclosed on quotes, " \
                        "e.g. 'exp_*_1.IDI*'." \
                        "Otherwise specify a space-separed list of FITS-IDI files."
    usage = "%(prog)s  [-h]  [--antab antabfile]  [--fits FITSIDIfiles]"
    description="Appends the Tsys and GC information from the ANTAB file(s) to the FITS-IDI files."
    parser = argparse.ArgumentParser(description=description, prog='append_antab_idi.py',
                                     usage=usage)
    parser.add_argument('-a', '--antab', type=str, default=None, help=help_antabfile)
    parser.add_argument('-f', '--fits', type=str, default=None, nargs='+', help=help_fitsidifiles)
    parser.add_argument('-r', '--replace', default=False, action='store_true',
                        help='Replaces Tsys/GC info if already present in the FITS-IDI files.')
    parser.add_argument('--version', action='version', version='%(prog)s {}'.format(__version__))
    arguments = parser.parse_args()

    if arguments.fits is not None:
        expname = arguments.fits[0].split('_')[0]
        if len(arguments.fits) == 1:
            idifiles = sorted(glob.glob(arguments.fits[0]), key=natsort_keygen())
        else:
            idifiles = arguments.fits
    else:
        expname = Path.cwd().name.lower()
        idifiles = sorted(glob.glob(f"{expname}_*_1.IDI*"), key=natsort_keygen())

    if len(idifiles) == 0:
        raise FileNotFoundError("The FITS-IDI file(s) could not be found.")

    if arguments.antab is not None:
        antabfiles = [arguments.antab]
    elif len(glob.glob(f"{expname}*.antab")) > 0:
        antabfiles = glob.glob(f"{expname}*.antab")
    else:
        pipepath = f"jops@archive.jive.eu:/data/pipe/{expname}/in/"
        print(f"Trying to recover {pipepath + expname}*.antab...")
        process = subprocess.call(["scp", "-r", pipepath + f"{expname}*.antab", "."], shell=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if process != 0:
            raise ValueError(f"No ANTAB files could be retrieved from the archive machine for {expname}.")

        antabfiles = sorted(glob.glob(f"{expname}*.antab"))
        print(f"Copied ANTAB file{'s' if len(antabfiles) > 1 else ''}:  {', '.join(antabfiles)}")

    # It could be just one pass, two passes with different antab files,
    # or multiple passes with same antab
    if len(antabfiles) == 1:
        main(antabfiles[0], idifiles, arguments.replace)
    else: #if len(antabfiles) == 2:
        for i in range(len(antabfiles)):
            main(antabfiles[i], [idi for idi in idifiles if f"_{i+1}_1.IDI" in idi])

    print('\nDone.')






