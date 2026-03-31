#!/bin/sh

# Ross contribution fixes leq 0 flagr values
sed -i -e 's/flagr,0/flagr,200/g' -e 's/flagr,-.00/flagr,200/g' *.log


#quick wrapper for uvflg..
for f in ./*.log; do
  uvflg.pl $f
done

ls -al *.uvflgfs
