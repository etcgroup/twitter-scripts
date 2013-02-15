#
# utils/helpers.py
# 
# 2013-02-14 	soco 	initial version
#

import simplejson


#
# helper functions
#

def pretty(obj):
	return simplejson.dumps(obj, sort_keys=True, indent=2)



def parse_twitter_date(datestr):
	return datetime.datetime.strptime(datestr[4:], '%b %d %H:%M:%S +0000 %Y')

