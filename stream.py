##
# stream.py - Collects tweets using the Twitter Streaming API.
#   Depends on tweepy and simplejson.
#
# Version 1.0, 2/3/2010 - use credentials and track list files (soco)
#
# Contributors:
#   <soco@uw.edu> John Robinson
#   <mjbrooks@uw.edu> Michael Brooks
##

import time
from getpass import getpass
from textwrap import TextWrapper
import tweepy
import re
import pprint
import sys
import simplejson
import argparse

#
#
# helper functions
#
#
def pretty(obj):
	return simplejson.dumps(obj, sort_keys=True, indent=2)


#
# reads single lines out f 
#
def read_track_list(filename):
    taglist = []
    with open(filename,"rt") as infile:
        for tag in infile:
            taglist.extend(tag.strip().split(','))
    return taglist


#
# reads credential file
#
def read_credential_file(filename):
    ret = {}
    with open(filename, "rt") as infile:
        for line in infile:
            if ":" in line:
                parts = line.split(':')
                ret[parts[0].strip()] = parts[1].strip()

    print pretty(ret)
    return ret

#
# interval
#
last_time = time.time()
tweets_in_interval = 0
INTERVAL = 2 * 60



#
#
# Check Args
#
#

# add args
parser = argparse.ArgumentParser()
parser.add_argument("credentialsfile", type=str, help="name of the credentials file")
parser.add_argument("--rateinterval", type=int, help="interval in seconds between rate updates",  default=INTERVAL)
parser.add_argument("--trackfile", help="file to read the tracklist from")




# parse args
args = parser.parse_args()

if args.rateinterval is not None:
    INTERVAL = args.rateinterval
    print >> sys.stderr, "rate notification interval: %d"%(INTERVAL)




#
#
# StreamListener
#
#
class StreamListener(tweepy.StreamListener):
    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')
    
    def on_data(self, data):
        global tweets_in_interval, INTERVAL, last_time
        print pretty(simplejson.loads(data)) +  ','
        tweets_in_interval += 1
        
        now = time.time()
        diff = now - last_time
        if diff > INTERVAL:
            last_time = now
            tweets_per_sec = tweets_in_interval / diff
            tweets_in_interval = 0
            print >> sys.stderr, 'Tweets per second:', tweets_per_sec

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return
    
    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return

def main():   
    
    credentials = read_credential_file(args.credentialsfile)

    # added this loop to power through exceptions and network/connection breaks
    while(1):

        auth1 = tweepy.auth.OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
        auth1.set_access_token(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'])
    
        l = StreamListener()
        streamer = tweepy.Stream(auth=auth1, listener=l, secure=True )
        track_list = [k for k in PRIMARY_TRACK_LIST.split(',')]
        track_list = [k for k in PRIMARY_TRACK_LIST.split(',')]
        if args.trackfile is not None:
            track_list = read_track_list(args.trackfile)
            print >> sys.stderr, "Tracking:", track_list


        try:
            streamer.filter(track = track_list)
        except Exception, e:
            #print "Sleeping!"
            print >> sys.stderr, e
            time.sleep(10)
    
#print "HERE\n"
        
        #streamer.sample() #- garden hose!!!


 
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print '\n'
#cursor.close()
#db.close()
#print '\nCiao!'


