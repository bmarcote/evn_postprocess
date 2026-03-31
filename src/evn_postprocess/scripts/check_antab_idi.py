#! /usr/bin/env python3
"""Checks if the Tsys and GC tables have been appended correctly to the FITS-IDI
for an EVN experiment.

Usage: check_antab_idi.py

Version: 1.0.1
Date: Apr 2023
Written by Benito Marcote (marcote@jive.eu)
"""
import os
import glob
import argparse
from pathlib import Path
from astropy.io import fits
from natsort import natsort_keygen
from rich import print as pprint

__version__ = 1.0

def has_Tsys(fitsfile):
    """Checks if the FITS-IDI file has the SYSTEM_TEMPERATURE table.
    """
    with fits.open(fitsfile) as hdu:
        return 'SYSTEM_TEMPERATURE' in hdu

def has_GC(fitsfile):
    """Checks if the FITS-IDI file has the GAIN_CURVE table.
    """
    with fits.open(fitsfile) as hdu:
        return 'GAIN_CURVE' in hdu


def check_consistency(fitsfile, verbose=True):
    """Check if all FITS-IDI files associated to an experiment has the right
    tables that they should have.

    Arguments
        - fitsfile : str
            FITS-IDI file name to check. It should be the first FITS-IDI file
            in case there are multiple (e.g. only exp_1_1.IDI1 even if there are
            multiple *.IDIn, n > 1).
            The rest of files would be not expected to have the tables.

    Returns
        - bool whenever everything is as expected.
    """
    if isinstance(fitsfile, str):
        fitsfile = Path(fitsfile)

    if not fitsfile.exists():
        raise FileNotFoundError(f"The FITS-IDI file {fitsfile} could not be found.")

    all_good = True
    if has_Tsys(fitsfile):
        if verbose:
            pprint(f"[green]{fitsfile} has SYSTEM_TEMPERATURE table.[/green]")
    else:
        if verbose:
            pprint(f"[red]{fitsfile} does not have SYSTEM_TEMPERATURE table.[/red]")

        all_good = False

    if has_GC(fitsfile):
        if verbose:
            pprint("[green]Has GAIN_CURVE table.[/green]")
    else:
        if verbose:
            pprint("[red]Does not have GAIN_CURVE table.[/red]")

        all_good = False

    return all_good


if __name__ == '__main__':
    description = "Checks if the Tsys and GC tables have been appended correctly to the FITS-IDI " \
                  "as needed for an EVN experiment."
    help_fitsidifiles = "FITS-IDI files to incorporate the ANTAB (Tsys and GC) information. " \
                        "You can use wildcards but then the string MUST be enclosed on quotes, " \
                        "e.g. 'exp_*_1.IDI*'." \
                        "Otherwise specify a space-separed list of FITS-IDI files."
    usage = "%(prog)s  [-h]  [--fits FITS-IDI-files]"
    parser = argparse.ArgumentParser(description=description, prog='check_antab_idi.py',
                                     usage=usage)
    parser.add_argument('-f', '--fits', type=str, default=None, nargs='+', help=help_fitsidifiles)
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    arguments = parser.parse_args()

    if arguments.fits is not None:
        if len(arguments.fits) == 1:
            idifiles = sorted(glob.glob(arguments.fits[0]), key=natsort_keygen())
        else:
            idifiles = arguments.fits
    else:
        idifiles = sorted(glob.glob(f"{Path.cwd().name.lower()}_*_1.IDI*"), key=natsort_keygen())

    files2check = [idi for idi in idifiles if (idi.endswith('.IDI') or idi.endswith('.IDI1')) \
                   and os.path.isfile(idi)]
    if len(files2check) == 0:
        raise FileNotFoundError("No FITS-IDI files found.")

    for afile in files2check:
        check_consistency(afile)
