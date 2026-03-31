#!/usr/bin/python2
#### ampcal-db.py
#### Jay Blanchard 2017 - python script to take an ampcal.dat file and add each line to the database.


import MySQLdb
import time
from datetime import datetime,timedelta
from warnings import filterwarnings


def insertDB(exp, date, source, telescope, freq, cor):

#basic SQL insert. If the below starts to fail this can be used (HOWEVER DUPLICATE CHECKING WILL HAVE TO BE DONE!)
#sql = """INSERT INTO ampcal(exp, obs_date, source, telescope, frequency, correction) VALUES ('%s', '%s', '%s', '%s', '%s', '%s');""" %(exp, date, source, telescope, freq, cor)

   #somewhat complicated SQL here to try to avoid duplicates.
   sql = """INSERT INTO ampcal(exp, obs_date, source, telescope, frequency, correction) SELECT * FROM(SELECT '%s', '%s', '%s', '%s', '%s', '%s') AS tmp 
   WHERE NOT EXISTS (SELECT * FROM ampcal WHERE exp='%s' AND source='%s' AND telescope='%s')
   LIMIT 1;""" % (exp, date, source, telescope, freq, cor, exp, source, telescope)

#   print sql
   try:
      cursor.execute(sql)
      db.commit()
   except (MySQLdb.Error, MySQLdb.Warning) as e:
      print(e)
      db.rollback()
   
#MAIN STARTS HERE
#open db con
db = MySQLdb.connect("db0.jive.eu","ampcal-w","klsge[09w4hj","ampcal")
cursor = db.cursor()

#ignore warnings about SQL above...
filterwarnings('ignore', category = MySQLdb.Warning)

print("Inserting ampcal.dat into database")
fin = open('ampcal.dat','rt')
for line in fin:
    col = line.split()
    year = int(float(col[1]))
    rem = float(col[1]) - year
    base = datetime(year, 1, 1)
    date = base + timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)
    unix_date = int((time.mktime(date.timetuple())+date.microsecond/1000000.0))
    try: # neeed to do this in a try for bad rows (easy way to avoid unreadable rows
        if  (float(col[5]) < 1.0): #we ignore any corrections > 1 as they are likely a bad source
            insertDB(col[0],date, col[2], col[3], col[4], col[5])
    except:
        pass;


fin.close()
#end - close db and file
db.close()
