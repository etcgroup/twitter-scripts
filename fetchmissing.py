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
import tweepy
from utils.helpers import *
from utils.twitter import *






#
#
# defines
#
#

DBHOST = 'localhost'
DBUSER = 'root'
DBPASS = None
DBNAME = 'twitter'

SELECT_INSTANCES_COUNT_QUERY = """select distinct count(t.in_reply_to_status_id) from tweets t where t.in_reply_to_status_id is not null and not exists (select * from tweets t2 where t2.id = t.in_reply_to_status_id)"""
SELECT_INSTANCES_QUERY = """select distinct t.in_reply_to_status_id from tweets t where t.in_reply_to_status_id is not null and not exists (select * from tweets t2 where t2.id = t.in_reply_to_status_id)"""


#
# helper functions
#
def pretty(obj):
	return simplejson.dumps(obj, sort_keys=True, indent=2)



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
parser.add_argument("credentialsfile", type=str, help="name of the credentials file")


	# parse args
args = parser.parse_args()

	# grab db password
if DBPASS is None:
	DBPASS = getpass.getpass('enter database password: ')




#
#
# main
#
#

print "Doing Auth"
auth = do_auth_from_file(args.credentialsfile)

api = tweepy.API(auth)

print "Connecting to db... (%s@%s %s)"%(args.dbuser,args.dbhost, args.dbname)
db = MySQLdb.connect(host=args.dbhost, user=args.dbuser, passwd=DBPASS, db=args.dbname, charset='utf8', use_unicode=True)
cursor = db.cursor(cursors.SSCursor)

cursor.execute(SELECT_INSTANCES_QUERY)
dbrow = cursor.fetchone() 
total = cursor.rowcount
print total
while dbrow is not None:
	print "id: %d"%( dbrow[0] )

	dbrow = cursor.fetchone()
	tweet = api.get_status(id=dbrow[0])
	print tweet.__dict__
	break

cursor.close()
db.close()



