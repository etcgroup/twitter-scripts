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



