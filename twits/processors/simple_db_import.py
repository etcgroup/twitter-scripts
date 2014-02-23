#! /usr/bin/env python
"""
A processing script that imports parsed tweets
into a MySQL table (schema shown below).
If the table does not exist, it will be created automatically.
"""

import MySQLdb
import HTMLParser
import sys
import traceback
import math
import getpass
import time

from tweet_processor import TweetProcessor
from ..utils.twitter import parse_twitter_date

CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS `%s` (
  `id` bigint(20) unsigned NOT NULL,
  `created_at` datetime NOT NULL,
  `text` varchar(255) CHARACTER SET utf8mb4 NOT NULL,
  `lat` float DEFAULT NULL,
  `lon` float DEFAULT NULL,
  `user_id` bigint(20) unsigned NOT NULL,
  `user_screen_name` varchar(100) NOT NULL,
  `user_name` varchar(100) NOT NULL,
  `user_location` varchar(150) DEFAULT NULL,
  `user_tz` varchar(150) DEFAULT NULL,
  `user_utc_offset` int(11) DEFAULT NULL,
  `user_geo_enabled` tinyint(1) NOT NULL DEFAULT '0',
  `retweet_of_status_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  KEY `created_at` (`created_at`),
  KEY `retweet_of_status_id` (`retweet_of_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
"""

# Default database settings
DBHOST = 'localhost'
DBUSER = 'root'
DBPASS = None
DBNAME = 'twitter'
DBTABLE = 'tweets'

BUFFER_SIZE = 1000
MAX_CACHE_SIZE = 50000
MIN_CACHE_SIZE = 25000

hp = HTMLParser.HTMLParser()

class SimpleDBImport(TweetProcessor):

    def __init__(self, *args, **kwargs):
        super(SimpleDBImport, self).__init__(*args, **kwargs)

        self.tweets_added = 0
        self.tweets_skipped = 0
        self.tweet_check_time = 0
        self.tweet_insert_time = 0

        self.tweet_buffer = {}
        self.tweet_cache = {}
        self.tweet_cache_hits = 0
        
        self.db = None
        self.cursor = None


    def arguments(self, parser):
        parser.add_argument("--dbhost", type=str, help="name of the database server", default=DBHOST)
        parser.add_argument("-u", "--dbuser", type=str, help="name of the database user", default=DBUSER)
        parser.add_argument("-p", "--dbpass", help="name of the database user", action='store_true')
        parser.add_argument("--dbname", type=str, help="name of the database schema", required=True)
        parser.add_argument("--dbtable", type=str, help="name of the table to insert into", default=DBTABLE)
        parser.add_argument("--nochecks", action='store_true', help="don't check for duplicate tweets. try to insert everything", default=False)

    def setup_queries(self):
        self.TWEET_INSERT_STMT = 'REPLACE INTO ' + self.args.dbtable + \
            ' (id, created_at, text, lat, lon, user_id, user_screen_name, user_name, user_location, user_tz, user_utc_offset, user_geo_enabled, retweet_of_status_id)' + \
            ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        self.TWEET_SELECT_STMT = 'SELECT id FROM ' + self.args.dbtable + ' WHERE id = (%s)'

        self.DISABLE_KEYS_STMT = 'ALTER TABLE ' + self.args.dbtable + ' DISABLE KEYS'
        self.ENABLE_KEYS_STMT = 'ALTER TABLE ' + self.args.dbtable + ' ENABLE KEYS'
        self.CREATE_TABLE_STMT = CREATE_TABLE_TEMPLATE % self.args.dbtable

    def setup(self):
        self.setup_queries()

        if self.args.dbpass:
            self.args.dbpass = getpass.getpass('enter database password: ')
        else:
            self.args.dbpass = ''
        
        try:
            self.db = MySQLdb.connect(
                host=self.args.dbhost,
                user=self.args.dbuser,
                passwd=self.args.dbpass,
                db=self.args.dbname,
                charset='utf8'
                )

        except Exception, e:
            print "Error connecting to db: ", e
            traceback.print_exc(file=sys.stderr)
            
        # Trick MySQLdb into using 4-byte UTF-8 strings
        self.db.query('SET NAMES "utf8mb4"')

        # Create the table if it doesn't exist
        self.db.query(self.CREATE_TABLE_STMT)


        # Disable keys for fast inserts
        self.db.query(self.DISABLE_KEYS_STMT)
        print "Database keys disabled."

        self.cursor = self.db.cursor()


    def process(self, tweet, raw_tweet):
        tweet_id = tweet['id']
        
        if self.tweet_in_db(tweet_id):
            self.tweets_skipped += 1
        else:
            tweet_created_at = parse_twitter_date(tweet['created_at'])
            tweet_text = hp.unescape(tweet['text'])

            retweet_id = tweet['retweeted_status']['id'] if 'retweeted_status' in tweet else None
            if retweet_id is not None:
                self.process(tweet['retweeted_status'], None)
                
            user = tweet['user']
            user_id = user['id']
            user_screen_name = user['screen_name']
            user_name = user['name']
            user_location = user['location']
            user_time_zone = user['time_zone']
            user_utc_offset = user['utc_offset']
            user_geo_enabled = user['geo_enabled']
            
            coordinates = tweet['coordinates']
            tweet_lat = None
            tweet_lon = None
            if coordinates:
                tweet_lon = coordinates['coordinates'][0]
                tweet_lat = coordinates['coordinates'][1]
                
            tweet_params = (
                tweet_id,
                tweet_created_at,
                tweet_text,
                tweet_lat,
                tweet_lon,
                user_id,
                user_screen_name,
                user_name,
                user_location,
                user_time_zone,
                user_utc_offset,
                user_geo_enabled,
                retweet_id,
                )
            
            # Insert the tweet
            self.buffer_tweet(tweet_id, tweet_params)


    def print_progress(self):
        print "  Tweets: {0:0.3f}s (check) {1:0.3f}s (insert)".format(self.tweet_check_time, self.tweet_insert_time)

        print "--- Counts ---"
        print "  inserted:", self.tweets_added, "tweets"
        print "    cached:", len(self.tweet_cache), "tweets"
        print "cache hits:", self.tweet_cache_hits, 'tweets'
        print "   skipped:", self.tweets_skipped, "tweets,"


    def teardown(self):
        try:
            self.flush_tweet_buffer()
        finally:
            print "--- Enabling keys... ---"
            self.db.query(self.ENABLE_KEYS_STMT)
            self.db.close()
        

    def is_in_cache(self, id):
        """Returns true if the id is in the cache."""
        if id in self.tweet_cache:
            # increment the hit count
            self.tweet_cache_hits += 1
            self.tweet_cache[id] += 1
            return True
        return False

    def add_to_cache(self, id):
        """Add a tweet id to the cache."""
        self.tweet_cache[id] = 1

        if len(self.tweet_cache) > MAX_CACHE_SIZE:
            self.truncate_cache()

            
    def truncate_cache(self):
        """Reduces the cache to MIN_CACHE_SIZE"""
        new_cache = {}

        entries = sorted(self.tweet_cache.iteritems(),
                         key=lambda pair: pair[1], reverse=True)

        for pair in entries:
            new_cache[pair[0]] = math.log(pair[1]) + 1
            
            if len(new_cache) > MIN_CACHE_SIZE:
                break

        self.tweet_cache = new_cache

    def tweet_in_db(self, tweet_id):
        """Returns true if the tweet is or will soon be in the db"""
        
        if self.args.nochecks:
            # we assume it is a brand new tweet to save time
            return False

        # First check in the buffer
        if tweet_id in self.tweet_buffer:
            return True

        # Then in the cache
        if self.is_in_cache(tweet_id):
            return True

        # Then in the db
        start = time.time()
        self.cursor.execute(self.TWEET_SELECT_STMT, [tweet_id])
        row = self.cursor.fetchone()
        self.tweet_check_time += time.time() - start

        if row is not None:
            self.add_to_cache(tweet_id)
            return True
        return False

    def buffer_tweet(self, tweet_id, tweet_data):
        self.tweet_buffer[tweet_id] = tweet_data

        if not self.args.nochecks:
            # only buffer if we are doing checks
            self.add_to_cache(tweet_id)

        if len(self.tweet_buffer) > BUFFER_SIZE:
            self.flush_tweet_buffer()


    def flush_tweet_buffer(self):

        if len(self.tweet_buffer) > 0:
            
            try:
                start = time.time()
                self.cursor.executemany(self.TWEET_INSERT_STMT, self.tweet_buffer.values())
                self.tweet_insert_time += time.time() - start
                
                self.tweets_added += len(self.tweet_buffer)
                self.tweet_buffer.clear()

            except Exception, e:
                print "Error inserting tweets: ", e


if __name__ == '__main__':
    processor = SimpleDBImport()
    processor.run()
