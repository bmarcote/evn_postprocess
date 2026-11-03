#!/usr/bin/env python3

import sys
import re

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage: {} <lis file>".format(sys.argv[0]))
        sys.exit(1)

    # example line:
    # + 26115/EG102H_No0040.cor   - No0040 No0190 64 1.000 Xpol ...
    line_re = re.compile(r"\+\s+\S+\s+-\s+[^\W\d]*(?P<start_scan>\d+)\s+"
                         r"[^\W\d]*(?P<end_scan>\d+)\s+(?P<n_lags>\d+)\s+"
                         r"(?P<t_int>\d*\.?\d*)\s+(?P<pol>\w+)")
    
    for filename in sys.argv[1:]:
        try:
            with open(filename, "r") as f:
                lines = f.readlines()
        except Exception as e:
            print("Error reading lis file {}: {}".format(filename, e))
    
        results = []
        for line in lines:
            if line.startswith("+"):
                match = line_re.match(line)
                if match is None:
                    print("Something unexpected in the following line:\n{}".\
                          format(line))
                    sys.exit(1)
                r = match.groupdict()
                r["start_scan"] = int(r["start_scan"])
                r["end_scan"] = int(r["end_scan"])
                r["n_lags"] = int(r["n_lags"])
                r["t_int"] = float(r["t_int"])
                results.append(r)

        if len(results) == 0:
            print("No scans in lis file {}".format(filename))
            continue
        
        print("  First scan = {}".format(results[0]["start_scan"]))
        for scan, next_scan in zip(results[:-1], results[1:]):
            skipped = next_scan["start_scan"] - scan["end_scan"] - 1
            if skipped > 0:
                print(" **** Skipped scans between {} and {}".format(
                    scan["end_scan"], next_scan["start_scan"]))
            elif skipped < 0:
                print(" **** Duplicated scans between {} and {}".format(
                    next_scan["start_scan"], scan["end_scan"]))
            
            for k in ["n_lags", "t_int", "pol"]:
                if scan[k] != results[-1][k]:
                    print("#### {} in scans {} - {} differs from last".format(
                        k, scan["start_scan"], scan["end_scan"]))
        print("   Last scan = {}".format(results[-1]["end_scan"]))
        
