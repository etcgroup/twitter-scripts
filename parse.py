#
# parse.py
#
# parses a list of twitter json messages (separated by a comma) and inserts them into a db
#
# 2013-01-21 soco
# 2013-01-31 mjbrooks
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


#
#
# defines
#
#

DBHOST = 'localhost'
DBUSER = 'root'
DBPASS = None
DBNAME = 'twitter_sagawards'

USER_INSERT_STMT = """INSERT INTO users 
			(id,screen_name,name,created_at,location,utc_offset,lang,time_zone,statuses_count) 
			VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

TWEET_INSERT_STMT = """INSERT INTO tweets
			(id,user_id,created_at,in_reply_to_status_id,in_reply_to_user_id, retweet_of_status_id, text, followers_count, friends_count, json) 
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

HASHTAG_INSERT_STMT = """INSERT INTO hashtags (string) VALUES (%s)"""
ENTITY_INSERT_STMT = """INSERT INTO entities (tweet_id, entity_id, entity_type, `from`, `to`) VALUES (%s,%s,%s,%s,%s)"""


#
# helper functions
#
def pretty(obj):
	return simplejson.dumps(obj, sort_keys=True, indent=2)


def parse_twitter_date(datestr):
	return datetime.datetime.strptime(datestr[4:], '%b %d %H:%M:%S +0000 %Y')

#
# check args
#
infilename = None
if sys.argv is not None and len(sys.argv) > 1:
    infilename = sys.argv[1]
else:
    print "Usage: parse <infilename>"
    quit()


if DBPASS is None:
	DBPASS = getpass.getpass('enter database password: ')

# ================================================================
#
#
# begin main
#
#
# ================================================================

print "Parsing %s..."%(infilename)
tweets = []
with open(infilename, "rt") as infile:
	tweet = ''
	tweet_start_found = False
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
			tweets.append(obj)
		elif tweet_start_found == True:
			# some line in the middle
			tweet += line




users = {}
tweets_to_insert = {}
entities = {}
hashtags = {}
mentions = {}

try:
	print "Pulling out retweets..."
	for tweet in tweets:
		#length = len(tweet['text'].encode('utf-8'))
		#if length > 140:
		#	print "%d '%s' (%d)"%(tweet['id'], tweet['text'].encode('utf-8'), length)
		if 'user' in tweet:
			if 'retweeted_status' not in tweet:
				continue
			retweet = tweet['retweeted_status']
			if retweet is not None:
				print 'adding retweet'
				tweets.append(retweet)

	print "Processing tweets..."
	for tweet in tweets:
		# sanity check as some json results are error messages such as the disconnected message
		if 'user' in tweet:
			tweet_id = tweet['id']
			created_at = parse_twitter_date(tweet['created_at'])
			user = tweet['user']
			user_id = user['id']
			user_created_at = parse_twitter_date(user['created_at'])

			#print "%s created at '%s'"%(user['screen_name'], created_at)

			#if tweet['in_reply_to_status_id'] is not None:
			#	print tweet['in_reply_to_status_id']

			#
			# users: build unique users list
			#
			if user_id not in users:
				users[user_id] = (
					user_id, 
					user['screen_name'], 
					user['name'], 
					user_created_at, 
					user['location'], 
					user['utc_offset'], 
					user['lang'], 
					user['time_zone'], 
					user['statuses_count'])

			#
			# tweets: build unique tweets list
			#

			retweet_id = None
			if 'retweeted_status' in tweet:
				retweet_id = tweet['retweeted_status']['id']
				


			if tweet_id not in tweets_to_insert:
				tweets_to_insert[tweet_id] = (
					tweet_id,
					user_id,
					created_at,
					tweet['in_reply_to_status_id'],
					tweet['in_reply_to_user_id'],
					retweet_id,
					tweet['text'],
					user['followers_count'],
					user['friends_count'],
					simplejson.dumps(tweet)
					)

			#
			# hash tags: build unique hashtag list
			#

			entities = tweet['entities']
			if 'hashtags' in entities:
				tweet_hashtags = entities['hashtags']
				for ht in tweet_hashtags:
					httxt = ht['text'].lower()
					if httxt not in hashtags:
						obj = {}
						obj['id'] = -1
						obj['tweets'] = { tweet_id: tweet}
						obj['indices'] = ht['indices']
						hashtags[httxt] = obj
					else:
						hashtags[httxt]['tweets'][tweet_id] = tweet

			#
			# mentions: build unique mentions list
			#
			if 'user_mentions' in entities:
				user_mentions = entities['user_mentions']
				for m in user_mentions:
					m_uid = m['id']
					if m_uid not in mentions:
						obj = {}
						obj['id'] = -1
						obj['tweets'] = { tweet_id: tweet}
						obj['indices'] = m['indices']
						mentions[m_uid] = obj
					else:
						mentions[m_uid]['tweets'][tweet_id] = tweet


except Exception, e:
	print e


# ==========================================================================
#
#
# insert into database
#
#
# ==========================================================================

try:


	print "Connecting to db..."
	db = MySQLdb.connect(host=DBHOST, user=DBUSER, passwd=DBPASS, db=DBNAME, charset='utf8', use_unicode=True)
	cur = db.cursor()


	#
	# insert users
	#
	print "Adding %d users..."%(len(users))
	cur.executemany( USER_INSERT_STMT, 
		users.values() )
	cur.close()

	#
	# insert tweets
	#

	print "Adding %d tweets..."%(len(tweets_to_insert))
	cur = db.cursor()

	cur.executemany( TWEET_INSERT_STMT,
			tweets_to_insert.values() )
	#for t in tweets_to_insert.values():
	#		print t
	cur.close()


	#
	# insert hashtags
	#
	print "Adding %d hashtags..."%(len(hashtags))
	cur = db.cursor()

	for htname, ht in hashtags.items():
		cur.execute(HASHTAG_INSERT_STMT, htname)
		ht['id'] = cur.lastrowid	#store the insert id
		#print "    Adding %d entity links from tweets to hashtags..."%(len(ht['tweets']))
		entities = []
		for tweet in ht['tweets'].values():
			entity = (
				tweet['id'],
				cur.lastrowid,
				'hashtag',
				ht['indices'][0],
				ht['indices'][1]
				)
			#print entity
			entities.append(entity)
		cur.executemany( ENTITY_INSERT_STMT, entities)
	cur.close()


	#
	# insert mentions
	#
	print "Adding %d mentions"%len(mentions)
	cur = db.cursor()

	for k, mention in mentions.items():
		entities = []
		for tweet in mention['tweets'].values():
			entity = (
				tweet['id'],
				k,
				'mention',
				mention['indices'][0],
				mention['indices'][1]
				)
			entities.append(entity)
		cur.executemany( ENTITY_INSERT_STMT, entities)

except Exception, e:
	print "Exception: ",e

finally:
	cur.close()


db.close()

#print pretty(users)


