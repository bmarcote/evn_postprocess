#!/usr/bin/env python3
import glob
import argparse
import subprocess
import shutil
from pathlib import Path


def main(args):
    if '/' in args.msfile:
        msfile = Path(args.msfile)
    else:
        msfile = Path.cwd() / Path(args.msfile)

    if msfile.exists():
        if args.delete:
            shutil.rmtree(msfile)
        else:
            raise FileExistsError(f"{msfile.name} exists and will not be overwritten (remove or use --delete option).")

    if ',' in args.idifiles:
        fitsidis = args.idifiles.split(',')
    else:
        fitsidis = sorted(glob.glob(args.idifiles))
    if len(fitsidis) == 0:
        raise FileNotFoundError(f"No FITS IDI files with name '{args.idifiles}' have been found.")

    # print(f"msfile: {msfile}")
    # print(f"args.idifiles: {args.idifiles}")
    # print(f"fitsidis: {fitsidis}")
    casascript = f"idi2ms-{msfile.name.replace('.ms', '')}.py"
    # fitsidis = [f'{f}' for f in fitsidis]
    with open(casascript, mode='w') as tfile:
        tfile.write(f"importfitsidi(vis='{str(msfile)}', fitsidifile={fitsidis}, " \
                    "constobsid=True, scanreindexgap_s=8.0, specframe='GEO')")

    process = subprocess.Popen(["/home/jops/bin/casa-5.7", "--nogui", "--nologger", "-c", casascript], stdout=None,
                               stderr=subprocess.STDOUT)

    while process.poll() is None:
        pass


if __name__ == '__main__':
    usage = "%(prog)s [-h] [--delete]  <msfile>  <idifiles>"
    description = """Converts a set of FITS IDI files into a single MS file by using the 'importfitsidi()'
    function from CASA, with the standard parameters expected for EVN data.

    Arguments:
      - msfile : str
        Name of the MS file to be created.
      - idifiles : str
        FITS IDI files to be read (either a comma-separated list or a single word using wildcards).
        In the later, explicit quotes (') are required before and after the string.
    """
    help_idifiles = "FITS IDI files to be read (either a comma-separated list or a single word " \
                    "using wildcards). In the later, explicit quotes (') are required before and " \
                    "after the string."
    help_delete = "If exists, then overwrites the MS output file. By default False (fails if MS exists)"

    parser = argparse.ArgumentParser(description=description, prog='idi2ms.py', usage=usage)
    parser.add_argument('msfile', type=str, help='Name of the MS file to be created.')
    parser.add_argument('idifiles', type=str, help=help_idifiles)
    parser.add_argument('-d', '--delete', action='store_true', default=False, help=help_delete)

    args = parser.parse_args()

    try:
        main(args)
    except FileExistsError as e:
        print('\n\n\n########### ERROR')
        print(f"FileExistsError: {e}")











