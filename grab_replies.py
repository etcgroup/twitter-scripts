#
# fetchmissing.py
#
# 2013-02-15
#
# soco
# 
#
#


import os
import sys
import simplejson
import datetime
import MySQLdb
from MySQLdb import cursors
import getpass
import argparse
import time
from utils.helpers import *
import csv







#
#
# defines
#
#

DBHOST = 'localhost'
DBUSER = 'root'
DBPASS = None
DBNAME = 'twitter'

SELECT_REPLIES_QUERY = """select id, in_reply_to_status_id from tweets where in_reply_to_status_id is not null"""




#
#
# Check Args
#
#

	# add args
parser = argparse.ArgumentParser()
parser.add_argument("--dbhost", type=str, help="name of the database server", default='localhost')
parser.add_argument("-u", "--dbuser", type=str, help="name of the database user", default='root')
parser.add_argument("-p", "--dbpass", help="name of the database user", action='store_true')
parser.add_argument("--dbname", type=str, help="name of the database schema", required=True)
parser.add_argument("csvfile", type=str, help="name of the csv file to write connections to")


	# parse args
args = parser.parse_args()

	# grab db password
if args.dbpass:
	args.dbpass = getpass.getpass('enter database password: ')
else:
	args.dbpass = ''


#
#
# main
#
#

print "Connecting to db... (%s@%s %s)"%(args.dbuser,args.dbhost, args.dbname)
db = MySQLdb.connect(host=args.dbhost, user=args.dbuser, passwd=args.dbpass, db=args.dbname, charset='utf8', use_unicode=True)
cursor = db.cursor()

cursor.execute(SELECT_REPLIES_QUERY)
dbrow = cursor.fetchone() 
total = cursor.rowcount
print "Total pairs: %d"%total

if total > 0:
	print "Writing to %s ..."%(args.csvfile)
	with open(args.csvfile, 'wb') as test_file:
		file_writer = csv.writer(test_file)
		while dbrow is not None:
			file_writer.writerow((dbrow[0],dbrow[1]))
			dbrow = cursor.fetchone()


		cursor.close()
		db.close()




