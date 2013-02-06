#
# parse.py
#
# parses a list of twitter json messages (separated by a comma) and inserts them into a db
#
# 2013-01-21 soco
# 2013-02-05 mjbrooks
#
# GOTCHAs:
#	* not all json objects in the file are valid status updates
#	* in_reply_to isn't always set when it's a reply
# 	* hashtags can appear more than once in a message (probably user mentions too)
#	* not all usernames or tweet ids will be in the dataset


import os
import sys
import simplejson
import datetime
import MySQLdb
import getpass
import argparse


#
#
# defines
#
#
PARSE_SIZE = 8 * 1024 * 1024

USER_STUB_INSERT_STMT = """REPLACE INTO users 
			(id,screen_name,name) 
			VALUES (%s,%s,%s)"""
USER_INSERT_STMT = """REPLACE INTO users 
			(id,screen_name,name,created_at,location,utc_offset,lang,time_zone,statuses_count) 
			VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
USER_SELECT_STMT = """SELECT id from users where id = (%s) and created_at IS NOT NULL"""

TWEET_INSERT_STMT = """REPLACE INTO tweets
			(id,user_id,created_at,in_reply_to_status_id,in_reply_to_user_id, retweet_of_status_id, text, followers_count, friends_count, json) 
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
TWEET_SELECT_STMT = """SELECT id FROM tweets WHERE id = (%s)"""

HASHTAG_INSERT_STMT = """INSERT INTO hashtags (string) VALUES (%s)"""
HASHTAG_SELECT_STMT = """SELECT id FROM hashtags where `string` = %s"""
HASHTAG_USES_INSERT_STMT = """REPLACE INTO hashtag_uses (tweet_id, hashtag_id) VALUES (%s,%s)"""
MENTIONS_INSERT_STMT = """REPLACE INTO mentions (tweet_id, user_id) VALUES (%s,%s)"""

#
# globals
#
hashtags = {}

#
# helper functions
#

def parse_twitter_date(datestr):
	return datetime.datetime.strptime(datestr[4:], '%b %d %H:%M:%S +0000 %Y')

	
DBHOST = 'localhost'
DBUSER = 'root'
DBPASS = 'blahhalb'
DBNAME = 'twitter'

parser = argparse.ArgumentParser()
parser.add_argument("--dbhost", type=str, help="name of the database server", default='localhost')
parser.add_argument("-u", "--dbuser", type=str, help="name of the database user", default='root')
parser.add_argument("-p", "--dbpass", help="name of the database user", action='store_true')
parser.add_argument("--dbname", type=str, help="name of the database schema", required=True)
parser.add_argument("tweetsfile", type=str, help="name of the file containing tweets")

#
# check args
#
args = parser.parse_args()

if args.dbpass:
	args.dbpass = getpass.getpass('enter database password: ')
else:
	args.dbpass = ''

#
# db funcs
#

users_added = 0
hashtags_added = 0
tweets_added = 0
mentions_added = 0
hashtag_uses_added = 0
users_skipped = 0
tweets_skipped = 0
hashtags_skipped = 0

#
#
#
def add_user_to_db(cursor, user, user_id):
	global users_added, users_skipped

	cursor.execute(USER_SELECT_STMT, user_id)
	row = cursor.fetchone()
	if row is not None:
		users_skipped += 1
	else:
		users_added += 1
		
		user_created_at = None
		user_location = None
		user_utc_offset = None
		user_lang = None
		user_time_zone = None
		user_statuses_count = None
		
		# If this is a full-fledged user record then all this will be present
		if 'created_at' in user:
			user_created_at = parse_twitter_date(user['created_at'])
			user_location = user['location']
			user_utc_offset = user['utc_offset']
			user_lang = user['lang']
			user_time_zone = user['time_zone']
			user_statuses_count = user['statuses_count']
		
			user_params = (
				user_id, 
				user['screen_name'], 
				user['name'], 
				user_created_at, 
				user_location, 
				user_utc_offset, 
				user_lang, 
				user_time_zone, 
				user_statuses_count)

			cursor.execute(USER_INSERT_STMT, user_params)
		else:
			user_params = (
				user_id,
				user['screen_name'], 
				user['name'])
			
			cursor.execute(USER_STUB_INSERT_STMT, user_params)
			
#
#
#
def get_or_add_hashtag(cursor, hashtag):
	global hashtags_added, hashtags_skipped
	
	httxt = hashtag['text'].lower()
	ht_id = -1
	if httxt not in hashtags:
		cursor.execute(HASHTAG_SELECT_STMT, httxt)
		row = cursor.fetchone()
		if row is None:
			cursor.execute(HASHTAG_INSERT_STMT, httxt)
			ht_id = cursor.lastrowid	#store the insert id
			hashtags_added += 1
		else:
			ht_id = row[0]
			hashtags_skipped += 1
		
		if ht_id == -1:
			print "bad hashtag id for %s"%(httxt)
		else:
			hashtags[httxt] = ht_id
	else:
		ht_id = hashtags[httxt]

	return ht_id

#
#
#
def add_tweet_to_db(cursor, tweet, raw_tweet):
	global hashtag_uses_added, tweets_added, mentions_added
	global tweets_skipped
	
	if 'user' in tweet:
		tweet_id = tweet['id']
		
		cursor.execute(TWEET_SELECT_STMT, tweet_id)
		row = cursor.fetchone()
		if row is not None:
			tweets_skipped += 1
		else:
			created_at = parse_twitter_date(tweet['created_at'])
			user = tweet['user']
			user_id = user['id']

			retweet_id = tweet['retweeted_status']['id'] if 'retweeted_status' in tweet else None

			tweet_params = (
				tweet_id,
				user_id,
				created_at,
				tweet['in_reply_to_status_id'],
				tweet['in_reply_to_user_id'],
				retweet_id,
				tweet['text'],
				user['followers_count'],
				user['friends_count'],
				raw_tweet
				)
				
			# Insert the tweet
			try:
				cursor.execute(TWEET_INSERT_STMT, tweet_params)
				tweets_added += 1
			except Exception, e:
				print "Error inserting tweet: ", e
			
			# Insert the user for the tweet
			add_user_to_db(cursor, user, user_id)

			#
			# Process the tweet's entities (mentions and hashtags)
			#
			entities = tweet['entities']

			if 'hashtags' in entities:
				tweet_hashtags = entities['hashtags']
				hashtags_in_tweet = set()
				for ht in tweet_hashtags:
					# make sure we haven't processed this already
					# since hashtags can appear multiple times per tweet
					htname = ht['text'].lower()
					if htname not in hashtags_in_tweet:
						hashtags_in_tweet.add(htname)
						ht_id = get_or_add_hashtag(cursor,ht)

						try:
							cursor.execute(HASHTAG_USES_INSERT_STMT, (tweet_id, ht_id))
							hashtag_uses_added += 1
						except Exception, e:
							print "Error inserting hashtag use: %s", e

			if 'user_mentions' in entities:
				tweet_mentions = entities['user_mentions']
				mentions_in_tweet = set()
				for mention in tweet_mentions:
					m_user_id = mention['id']
					if m_user_id not in mentions_in_tweet:
						mentions_in_tweet.add(m_user_id)

						try:
							cursor.execute(MENTIONS_INSERT_STMT, (tweet_id, m_user_id))
							mentions_added += 1
						except Exception, e:
							print "Error inserting mention: %s", e
							
						add_user_to_db(cursor, mention, m_user_id)

	return None


def print_progress():
	print "inserted:", tweets_added, "tweets,", users_added, "users,"
	print "         ", hashtags_added, "hashtags,", mentions_added, "mentions,", hashtag_uses_added, "hashtag uses"
	print "  cached:", len(hashtags), "hashtags"
	print " skipped:", tweets_skipped, "tweets,", users_skipped, "users,"
	print "         ", hashtags_skipped, "hashtags"
	
# ================================================================
#
#
# begin main
#
#
# ================================================================


# db connect
print "Connecting to db..."
try:
	db = MySQLdb.connect(
			host=args.dbhost, 
			user=args.dbuser, 
			passwd=args.dbpass, 
			db=args.dbname,
			charset='utf8')
	
	# Trick MySQLdb into using 4-byte UTF-8 strings
	db.query('SET NAMES "utf8mb4"')
	
	cur = db.cursor()
except Exception, e:
	print "Error connecting to db: ",e
	quit()

print "Parsing %s..."%(args.tweetsfile)
tweets = []
with open(args.tweetsfile, "rt") as infile:
	# grab file size
	infile.seek(0,os.SEEK_END)
	filesize = infile.tell()
	infile.seek(0,os.SEEK_SET)

	# start our read loop with valid data
	tweet = ''
	tweet_start_found = False
	last_parse_position = 0
	for line in infile:

		if line[0] == '{':
			# start of tweet
			tweet_start_found = True
			tweet = ''
			tweet += line
		elif line[0:2] == '},' and tweet_start_found == True:
			# end of tweet
			tweet += line[0]
			tweet_start_found = False
			obj = simplejson.loads(tweet)
			#print obj
			add_tweet_to_db(cur, obj, tweet)
			#tweets.append(obj)
		elif tweet_start_found == True:
			# some line in the middle
			tweet += line

		cur_pos = infile.tell()
		if (cur_pos - last_parse_position) > PARSE_SIZE:
			last_parse_position = cur_pos
			pct_done = (float(cur_pos) * 100.0 / float(filesize))
			print "===================="
			print "%f%% complete..."%(pct_done)
			print_progress()
			
	print_progress()

db.close()
