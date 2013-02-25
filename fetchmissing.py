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
import time
from tweepy.error import TweepError
from tweepy.parsers import Parser
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

TWEET_INSERT_STMT = """REPLACE INTO tweets
			(id,user_id,created_at,in_reply_to_status_id,in_reply_to_user_id, retweet_of_status_id, text, followers_count, friends_count) 
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""


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
# classes
#
class RawJsonParser(Parser):
	def parse(self, method, payload):
		return payload






#
#
# main
#
#

# grab api
auth = do_auth_from_file(args.credentialsfile)	
api = tweepy.API(auth, parser=RawJsonParser())
print >> sys.stderr, pretty(simplejson.loads(api.rate_limit_status()))
print >> sys.stderr, "GMtime: ", time.mktime(time.gmtime())
print >> sys.stderr, "localtime: ", time.mktime(time.localtime())


print >> sys.stderr, "Connecting to db... (%s@%s %s)"%(args.dbuser,args.dbhost, args.dbname)
db = MySQLdb.connect(host=args.dbhost, user=args.dbuser, passwd=DBPASS, db=args.dbname, charset='utf8', use_unicode=True)
cursor = db.cursor()

cursor.execute(SELECT_INSTANCES_QUERY)
dbrow = cursor.fetchone() 
total = cursor.rowcount
print >> sys.stderr, "Total Tweets: %d"%total

#print total

status_count = 0

while dbrow is not None:
	tweet = None
	try:
		print >> sys.stderr, "grabbing %s"%(dbrow[0])
		tweet = api.get_status(id=dbrow[0])
	except TweepError, e:
		if e.response.status == 400:
			print >> sys.stderr, pretty(simplejson.loads(api.rate_limit_status()))
			print >> sys.stderr, "Sleeping"
			print >> sys.stderr, "GMtime: ", time.mktime(time.gmtime())
			time.sleep(10*60)
		elif e.response.status != 404 and e.response.status != 403:
			print "TweetExcept: (%s)(%s)"%(e.reason, e.response.status)
			quit()
	except Exception, e:
		print "Exception: (%s) %s"%(type(e),e)		
		quit()

	if tweet is not None:
		print tweet
		print >> sys.stderr, "%s,"%(pretty(simplejson.loads(tweet)))
	else:
		print >> sys.stderr, "error grabbing tweet %s"%(dbrow[0])


	status_count += 1
	if (status_count % 10) == 0:
		print >> sys.stderr, "%d of %d"%(status_count, total)

	dbrow = cursor.fetchone()


cursor.close()
db.close()



