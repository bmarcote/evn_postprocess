#!/usr/bin/env python3
import argparse
from collections import defaultdict
import numpy as np


def get_gscale_from_file(gscale_file, verbose=False):
    with open(gscale_file, 'r') as a_file:
        if verbose:
            print(f"Opened file {gscale_file}")

        content = ''.join(a_file.readlines())

    if verbose:
        print(f"Read content: {content}---")

    return get_gscale_from_text(content, verbose=verbose)


def get_gscale_from_text(gscale_text, verbose=False):
    ants = defaultdict(list)
    for a_line in gscale_text.split('\n'):
        if 'Correcting ' in a_line:
            if verbose:
                print(a_line)

            # an_if = a_line[a_line.index('IF ')+3:a_line.index('.')].strip()
        elif '  ' == a_line[:2] and len(a_line) > 0:
            values = [i.strip() for i in a_line.split()]
            for j in range(0, len(values), 2):
                if '*' not in values[j+1]:
                    ants[values[j]].append(values[j+1])

    if verbose:
        print(f"Antennas found: {', '.join(ants)}")

    for ant in ants:
        if len(ants[ant]) > 0:
            print(f"{ant}: {np.median([float(f) for f in ants[ant]]):.02}  --  {', '.join(ants[ant])}")
    return ants


if __name__ == '__main__':
    usage = '%(prog)s  [-h]  <filename>'
    description = "For a file containing the extracted information from Difmap Gscale, it will reformat it " \
                  "so it is per antenna, instead of per subband, and it determines the average gain factor correction."
    parser = argparse.ArgumentParser(description=description, prog='gscale2avg.py', usage=usage)
    parser.add_argument('filename', type=str, help="File containing the (cut) output from Difmap Gscale")
    parser.add_argument('-v', '--verbose', default=False, action='store_true', help='Verbose output')
    args = parser.parse_args()

    get_gscale_from_file(args.filename, verbose=args.verbose)



