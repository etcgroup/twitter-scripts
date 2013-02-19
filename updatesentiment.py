import requests
import simplejson
import os
import sys
import datetime
import MySQLdb
from MySQLdb import cursors
import getpass
import argparse


DBHOST = '127.0.0.1'
DBUSER = 'root'
DBPASS = ''
DBNAME = 'twitter'

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
parser.add_argument("--dbhost", help="Database host name", default=DBHOST)
parser.add_argument("--dbuser", help="Database user name", default=DBUSER)
parser.add_argument("--dbname", help="Database name", default=DBNAME)
parser.add_argument("--appId", help="Sentiment140 app id (email address)", required=True)
parser.add_argument("--requestSize", help="Number of tweets to request per batch", default=10000)
parser.add_argument("--all", help="Test all tweets, not just retweets", action='store_true')

# parse args
args = parser.parse_args()

# grab db password
if DBPASS is None:
    DBPASS = getpass.getpass('enter database password: ')

    
SELECT_INSTANCES_QUERY = """select t.id, t.text from tweets t where t.sentiment is null limit %s"""%(args.requestSize)
SELECT_RETWEETS_QUERY = """select t.id, t.text from tweets t where t.sentiment is null and t.in_reply_to_status_id is not null limit %s"""%(args.requestSize)
UPDATE_SENTIMENT_QUERY = """update tweets t set t.sentiment = %s where t.id = %s"""

#
#
# main
#
#

print "Connecting to db... (%s@%s %s)"%(args.dbuser,args.dbhost, args.dbname)
db = MySQLdb.connect(host=args.dbhost, user=args.dbuser, passwd=DBPASS, db=args.dbname, charset='utf8', use_unicode=True)
cursor = db.cursor(cursors.SSCursor)

count = 0
while(1):
    tweets = {"data": []}
    
    if args.all:
        cursor.execute(SELECT_INSTANCES_QUERY)
    else:
        cursor.execute(SELECT_RETWEETS_QUERY)
        
    dbrow = cursor.fetchone()
    if dbrow is None:
        break
    while dbrow is not None:
        tweets["data"].append({"id":dbrow[0], "text":dbrow[1]})
        dbrow = cursor.fetchone()


    url = "http://www.sentiment140.com/api/bulkClassifyJson?appid=" + args.appId

    print "Requesting sentiment for", len(tweets["data"]), "tweets"
    r = requests.post(url, data=simplejson.dumps(tweets))
    
    print r.text

    results = simplejson.loads(r.text)

    dbvals = []

    for t in results["data"]:
        num = (t["polarity"] - 2) / 2
        print t["id"], num
        dbvals.append([num, t["id"]])

    cursor.executemany(UPDATE_SENTIMENT_QUERY, dbvals)

    count = count + 1
    print "Batch", count, "completed."
    
cursor.close()
db.close()