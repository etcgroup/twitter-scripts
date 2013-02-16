#
# utils/twitter.py
# 
# 2013-02-14 	soco 	initial version
#

import sys
import tweepy
from helpers import *


#
# parse a twitter date
#
def parse_twitter_date(datestr):
    return datetime.datetime.strptime(datestr[4:], '%b %d %H:%M:%S +0000 %Y')

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

    #print pretty(ret)
    return ret




def do_auth(credentials):
	auth1 = tweepy.auth.OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
	auth1.set_access_token(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'])
	return auth1


def do_auth_from_file(filename):
	credentials = read_credential_file(filename)
	return do_auth(credentials)


