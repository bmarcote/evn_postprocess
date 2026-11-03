#!/usr/bin/env python3
import argparse
from collections import defaultdict
from pathlib import Path
import numpy as np


def get_gscale_from_file(gscale_file, output, verbose=True):
    with open(gscale_file, 'r') as a_file:
        gscale_text= ''.join(a_file.readlines())

    ants = defaultdict(list)
    for a_line in gscale_text.split('\n'):
        if 'Correcting ' in a_line:
            # if verbose:
            #     print(a_line)
            pass
            # an_if = a_line[a_line.index('IF ')+3:a_line.index('.')].strip()
        elif '  ' == a_line[:2] and len(a_line) > 0:
            values = [i.strip() for i in a_line.split()]
            for j in range(0, len(values), 2):
                if '*' not in values[j+1]:
                    ants[values[j]].append(values[j+1])

    if verbose:
        print(f"Antennas found: {', '.join(ants)}")

    to_write = []
    for ant in ants:
        if len(ants[ant]) > 0:
            to_write.append(f"{ant:4}: {np.median([float(f) for f in ants[ant]]):4.2} "
                            f"({np.median([float(f)**2 for f in ants[ant]]):.04})  --  {', '.join(ants[ant])}")

    
    if verbose:
        print('\n'.join(to_write))

    with open(output if output is not None else gscale_file.replace(Path(gscale_file).suffix, "-avg" + Path(gscale_file).suffix), 'w') as out:
        out.write('\n'.join(to_write))

    return ants


if __name__ == '__main__':
    usage = '%(prog)s  [-h]  <filename>'
    description = "For a file containing the extracted information from Difmap Gscale, it will reformat it " \
                  "so it is per antenna, instead of per subband, and it determines the average gain factor correction."
    parser = argparse.ArgumentParser(description=description, prog='gscale2avg.py', usage=usage)
    parser.add_argument('filename', type=str, help="File containing the (cut) output from Difmap Gscale")
    parser.add_argument('-o', '--output', default=None, type=str, help="File to store the Difmap Gscale values")
    parser.add_argument('-s', '--silent', default=False, action='store_true', help='skip verbose output')
    args = parser.parse_args()

    get_gscale_from_file(args.filename, args.output, verbose=not args.silent)



