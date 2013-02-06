##
# separate.py
#
# parses a list of twitter json messages (separated by a comma) and makes a new file containing
# only those that match a provided list of searches filter.
#
# Depends on simplejson
#
# 2013-01-31 mjbrooks
# 2013-01-21 soco
##


import os
import sys
import simplejson

#
# helper functions
#
def pretty(obj):
	return simplejson.dumps(obj, sort_keys=True, indent=2)

#
# check args
#
infilename = None
outfilename = None
searches = []

if sys.argv is not None and len(sys.argv) < 3:
	print "Usage: separate <infilename> [term list ...]"
	quit()

infilename = sys.argv[1]
for index in range(2, len(sys.argv)):
	searches.append(sys.argv[index].lower())

print >> sys.stderr, 'Displaying tweets matching', searches, 'from', infilename

# Checks if the tweet matches any of the search strings
def search_matches(tweet_json):
	tweet_text = tweet.lower()
	for hashtag in searches:
		if hashtag in tweet_text:
			return True
	return False

	
# ================================================================
#
#
# begin main
#
#
# ================================================================

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
			if search_matches(tweet):
				print tweet + ','
		elif tweet_start_found == True:
			# some line in the middle
			tweet += line
