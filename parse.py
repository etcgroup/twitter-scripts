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
import HTMLParser
import time
import heapq

#
#
# defines
#
#
PARSE_SIZE = 8 * 1024 * 1024

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
hp = HTMLParser.HTMLParser()

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
user_cache_hits = 0

user_check_time = 0
user_insert_time = 0
tweet_check_time = 0
tweet_insert_time = 0
hashtag_use_insert_time = 0
mention_insert_time = 0
hashtag_check_time = 0
hashtag_insert_time = 0
tweet_read_time = 0
tweet_process_time = 0
tweet_parse_time = 0
start_time = 0

tweet_buffer = {}
user_buffer = {}

TWEETS_TO_BUFFER = 100
USERS_TO_BUFFER = 100

user_cache = {}
MAX_CACHE_SIZE = 10000
MIN_CACHE_SIZE = 5000

# Return true if the user is in the cache
def user_in_cache(user_id):
	global user_cache_hits, user_cache
	
	if user_id in user_cache:
		# increment the hit count
		user_cache_hits += 1
		user_cache[user_id] += 1
		return True
	return False

def cache_user(user_id):
	global user_cache
	user_cache[user_id] = 1
	
	if len(user_cache) > MAX_CACHE_SIZE:
		truncate_user_cache()

def truncate_user_cache():
	global user_cache
	new_cache = {}
	
	entries = sorted(user_cache.iteritems(), key=lambda pair: pair[1], reverse=True)
	
	for pair in entries:
		new_cache[pair[0]] = pair[1] / 2
		
		if len(new_cache) > MIN_CACHE_SIZE:
			break
	
	user_cache = new_cache

# Return true if the user is in the db (or will be)
def user_in_db(cursor, user_id):
	global user_check_time
	
	# Check the buffer first
	if user_id in user_buffer:
		return True
	
	if user_in_cache(user_id):
		return True
	
	# Then check the db
	start = time.time()
	cursor.execute(USER_SELECT_STMT, user_id)
	row = cursor.fetchone()
	user_check_time += time.time() - start;
	
	return row is not None

# Add the user to the buffer, for later inserting into the db
def buffer_user(cursor, user_id, user_data):
	global user_buffer
	
	user_buffer[user_id] = user_data
	
	cache_user(user_id)
	
	if len(user_buffer) >= USERS_TO_BUFFER:
		flush_user_buffer(cursor)
	
# Insert all buffered users into the db
def flush_user_buffer(cursor):
	global user_buffer, users_added, user_insert_time
	
	if len(user_buffer) > 0:
	
		try:
			start = time.time()
			cursor.executemany(USER_INSERT_STMT, user_buffer.values())
			user_insert_time += time.time() - start
			
			users_added += len(user_buffer)
			user_buffer.clear()
		except Exception, e:
			print "Error inserting users: ", e
#
#
#
def add_user_to_db(cursor, user, user_id):
	global users_skipped

	if user_in_db(cursor, user_id):
		users_skipped += 1
	else:
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
		
		buffer_user(cursor, user_id, user_params)
#
#
#
def get_or_add_hashtag(cursor, hashtag):
	global hashtags_added, hashtags_skipped
	global hashtag_check_time, hashtag_insert_time
	
	httxt = hashtag['text'].lower()
	ht_id = -1
	if httxt not in hashtags:
		start = time.time()
		cursor.execute(HASHTAG_SELECT_STMT, httxt)
		row = cursor.fetchone()
		hashtag_check_time += time.time() - start
		
		if row is None:
			start = time.time()
			cursor.execute(HASHTAG_INSERT_STMT, httxt)
			ht_id = cursor.lastrowid	#store the insert id
			hashtag_insert_time += time.time() - start
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

# Return true if the tweet is in the db (or will be)
def tweet_in_db(cursor, tweet_id):
	global tweet_check_time
	
	# first check in buffer
	if tweet_id in tweet_buffer:
		return True
		
	# then check in db
	start = time.time()
	cursor.execute(TWEET_SELECT_STMT, tweet_id)
	row = cursor.fetchone()
	tweet_check_time += time.time() - start
	
	return row is not None
	
# Store the tweet for later inserting into the db
def buffer_tweet(cursor, tweet_id, tweet_data):
	global tweet_buffer
	
	tweet_buffer[tweet_id] = tweet_data
	
	if len(tweet_buffer) >= TWEETS_TO_BUFFER:
		flush_tweet_buffer(cursor)

# Insert all buffered tweets into the db
def flush_tweet_buffer(cursor):
	global tweet_insert_time, tweets_added, tweet_buffer

	if len(tweet_buffer) > 0:
		
		try:
			start = time.time()
			cursor.executemany(TWEET_INSERT_STMT, tweet_buffer.values())
			tweet_insert_time += time.time() - start
			
			tweets_added += len(tweet_buffer)
			tweet_buffer.clear()
		
		except Exception, e:
			print "Error inserting tweets: ", e
#
#
#
def add_tweet_to_db(cursor, tweet, raw_tweet):
	global hashtag_uses_added, mentions_added
	global tweets_skipped
	global mention_insert_time, hashtag_use_insert_time
	
	if 'user' in tweet:
		tweet_id = tweet['id']
		
		if tweet_in_db(cursor, tweet_id):
			tweets_skipped += 1
		else:
			if raw_tweet is None:
				raw_tweet = simplejson.dumps(tweet)
			
			created_at = parse_twitter_date(tweet['created_at'])
			user = tweet['user']
			user_id = user['id']

			retweet_id = tweet['retweeted_status']['id'] if 'retweeted_status' in tweet else None
			if retweet_id is not None:
				add_tweet_to_db(cursor, tweet['retweeted_status'], None)

			tweet_params = (
				tweet_id,
				user_id,
				created_at,
				tweet['in_reply_to_status_id'],
				tweet['in_reply_to_user_id'],
				retweet_id,
				hp.unescape(tweet['text']),
				user['followers_count'],
				user['friends_count'],
				raw_tweet
				)
				
			# Insert the tweet
			buffer_tweet(cursor, tweet_id, tweet_params)
			
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
							start = time.time()
							cursor.execute(HASHTAG_USES_INSERT_STMT, (tweet_id, ht_id))
							hashtag_use_insert_time += time.time() - start
							
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
							start = time.time()
							cursor.execute(MENTIONS_INSERT_STMT, (tweet_id, m_user_id))
							mention_insert_time += time.time() - start
							
							mentions_added += 1
						except Exception, e:
							print "Error inserting mention: %s", e
						
						# Don't add partial users
						# add_user_to_db(cursor, mention, m_user_id)

	return None


def print_progress():
	print "--- Counts ---"
	print "  inserted:", tweets_added, "tweets,", users_added, "users,"
	print "           ", hashtags_added, "hashtags,", mentions_added, "mentions,", hashtag_uses_added, "hashtag uses"
	print "    cached:", len(hashtags), "hashtags", len(user_cache), "users"
	print "cache hits:", user_cache_hits, "users"
	print "   skipped:", tweets_skipped, "tweets,", users_skipped, "users,"
	print "           ", hashtags_skipped, "hashtags"
	
	print "--- Timing {:0.3f}s (total) ---".format(time.time() - start_time)
	print "  Totals: {:0.3f}s (read) {:0.3f}s (parse) {:0.3f}s (process)".format(tweet_read_time, tweet_parse_time, tweet_process_time)
	print "  Tweets: {:0.3f}s (check) {:0.3f}s (insert)".format(tweet_check_time, tweet_insert_time)
	print "   Users: {:0.3f}s (check) {:0.3f}s (insert)".format(user_check_time, user_insert_time)
	print "Hashtags: {:0.3f}s (check) {:0.3f}s (insert)".format(hashtag_check_time, hashtag_insert_time)
	print "Entities: {:0.3f}s (mentions) {:0.3f}s (hashtags)".format(mention_insert_time, hashtag_use_insert_time)

	
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

	db.query('ALTER TABLE hashtag_uses DISABLE KEYS')
	db.query('ALTER TABLE mentions DISABLE KEYS')
	db.query('ALTER TABLE tweets DISABLE KEYS')
	print "Database keys disabled."
	
	cur = db.cursor()

	print "Parsing %s..."%(args.tweetsfile)

	with open(args.tweetsfile, "rt") as infile:
		start_time = time.time()
		# grab file size
		infile.seek(0,os.SEEK_END)
		filesize = infile.tell()
		infile.seek(0,os.SEEK_SET)
		
		# start our read loop with valid data
		tweet = ''
		tweet_start_found = False
		start = 0
		last_parse_position = 0
		for line in infile:

			if line[0] == '{':
				# start of tweet
				tweet_start_found = True
				start = time.time()
				tweet = ''
				tweet += line
			elif line[0:2] == '},' and tweet_start_found == True:
				# end of tweet
				tweet += line[0]
				tweet_start_found = False
				tweet_read_time += time.time() - start
				
				start = time.time()
				obj = simplejson.loads(tweet)
				tweet_parse_time += time.time() - start;
				
				start = time.time()
				add_tweet_to_db(cur, obj, tweet)
				tweet_process_time += time.time() - start
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

		flush_tweet_buffer(cursor)
		flush_user_buffer(cursor)
		
		print_progress()

except Exception, e:
	print "Error connecting to db: ",e
finally:
	print "--- Enabling keys... ---"
	db.query('ALTER TABLE hashtag_uses ENABLE KEYS')
	db.query('ALTER TABLE mentions ENABLE KEYS')
	db.query('ALTER TABLE tweets ENABLE KEYS')
	print "Done."
	db.close()
