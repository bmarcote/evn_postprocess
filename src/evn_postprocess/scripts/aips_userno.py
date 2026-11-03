#! /usr/bin/env python3
import argparse

description = """Returns the AIPS user number associated to the given support scientist,
and increases the stored number by one (so the next time it will be the following number).
"""

def main(person: str, aipsfile: str):
    supsciers = {}
    with open(aipsfile, 'r') as infile:
        for aline in infile.readlines():
            if ':' in aline:
                assert aline.count(':') == 1, "ERROR in the aips_userno.txt file. More than one value per line."
                supsci, aipsno = aline.split(':')
                supsci = supsci.strip()
                assert supsci.lower() not in supsciers, "ERROR: multiple entries for the same support scientist."
                supsciers[supsci] = int(aipsno.strip())

    aipsno = supsciers[person]
    supsciers[person] += 1
    with open(aipsfile, 'w') as outfile:
        for key,value in supsciers.items():
            outfile.write(f"{key}: {value}\n")
    print(aipsno)
    return aipsno 





if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-f', '--filename', type=str, default='/data/exp/aips_userno.txt',
                        help='The filename to read from (by default /data/exp/aips_userno.txt).')
    parser.add_argument('susci', type=str, help='The surname of the support scientist.')
    args = parser.parse_args()

    main(args.susci.lower(), args.filename)


