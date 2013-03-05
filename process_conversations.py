#
# cull graph
#
# attempts to read in a list of replies and remove all single replies (two nodes, 1 edge)
#

import os
import sys
import simplejson
import argparse
import random
import csv
import MySQLdb
from MySQLdb import cursors
import getpass
from utils.helpers import *


#
#
# consts
#
#

INSERT_CONVERSATION = """insert INTO conversations (breadth, depth, root_tweet, tweet_count) VALUES (%s,%s,%s,%s)"""
UPDATE_TWEET_QUERY = """update tweets t set t.conversation_id = %s, t.depth = %s, t.child_count = %s where t.id = %s"""
UPDATE_CONVERSATION_WITH_CALCS = """
update conversations c 
join 
	(select t.conversation_id as id, min(t.created_at) as start_date, 
		max(t.created_at) as end_date, 
		count(distinct t.user_id) as num_users, 
		sum(t.retweet_count)  as retweet_count,
		avg(t.sentiment) as sentiment
	from tweets t
	where t.conversation_id is not null
	group by t.conversation_id) a
on a.id = c.id
set c.`start` = a.start_date, c.`end` = a.end_date, c.users_count = a.num_users, c.retweet_count = a.retweet_count, c.sentiment = a.sentiment
where c.id = a.id
"""


DBHOST = 'localhost'
DBUSER = 'root'
DBPASS = None
DBNAME = 'twitter'

#
#
# Classes
#
#


class Conversation:
	def __init__(self, first):
		self.replies = []
		self.tweets = {}
		self.root = None
		self.depth = 0
		self.breadth = 0
		if first is not None:
			self.addReply(first[0],first[1])
			self.id = first[1]
		else:
			self.id = random.randrange(0,100000,1)

	def addTweet(self,tweet):
		if tweet not in self.tweets:
			self.tweets[tweet] = {'id': tweet, 'parent': None, 'children': [], 'depth': 0}

	def addReply(self,source,dest):
		self.replies.append((source,dest))
		# add tweets
		self.addTweet(source)
		self.addTweet(dest)

		# add parent-child relationships
		self.tweets[source]['parent'] = dest
		self.tweets[dest]['children'].append(source)


	def extend(self,other):
		for r in other.replies:
			self.addReply(r[0],r[1])


	def findroot(self):
		self.root = None
		for t in self.tweets.values():
			if t['parent'] is None:
				#if self.root is not None:
				#	print "root is already set!"
				self.root = t
				#print "Root = ", pretty(t)
		if self.root is None:
			print "No root found"


	def walkTree(self):
		""" recurisve tree walk begin function """
		if self.root is None:
			print "invalid root"
			return
		self.bfswalk([self.root],0)


	def bfswalk(self,nodelist,depth):
		children = []
		# find all children
		#print "bfswalk (depth:%d, nodelist:%d)"%(depth, len(nodelist))
		self.depth = max(depth,self.depth)
		self.breadth = max(len(nodelist), self.breadth)
		for n in nodelist:
			#print pretty(n)
			nodeChildren = [self.tweets[k] for k in n['children']]
			n['depth'] = depth
			#print "  depth %d, %d children"%(depth, len(nodeChildren))
			#print pretty(nodeChildren)
			children.extend(nodeChildren)
		if len(children) > 0:
			self.bfswalk(children, depth+1)



	def getChildren(self,id):
		node = self.tweets[tweet]
		if node is not None:
			return node['children']

	def process(self):
		self.findroot()
		self.walkTree()



	def __len__(self):
		return len(self.replies)

	def __hash__(self):
		return self.id

	def __eq__(self,other):
		return self.id == other.id

	def __ne__(self,other):
		return self.id != other.id

	def __cmp__(self,other):
		return self.id - other.id

	def __str__(self):
		return "%d (len:%d, root:%d, depth:%d, breadth:%d)"%(self.id,len(self.replies), self.root['id'], self.depth, self.breadth)

#
#
# Check Args
#
#

# add args
parser = argparse.ArgumentParser()
parser.add_argument("infile", type=str, help="name of the input edge csv file")
parser.add_argument("--dbhost", help="Database host name", default=DBHOST)
parser.add_argument("--dbuser", help="Database user name", default=DBUSER)
parser.add_argument("--dbname", help="Database name", required=True)
parser.add_argument("-p", "--dbpass", help="name of the database user", action='store_true')
parser.add_argument("--min_width", type=int, help="minimum width of the tree to dump", default=1)
parser.add_argument("--min_depth", type=int, help="minimum width of the tree to dump", default=1)
parser.add_argument("--min_messages", type=int, help="minimum width of the tree to dump", default=2)
# parse args
args = parser.parse_args()

if args.dbpass:
	args.dbpass = getpass.getpass('enter database password: ')
else:
	args.dbpass = ''


#
#
#
# main
#
#
#
tweets = {}
convs = set()

cnt = 0
with open(args.infile, "rt") as infile:
	for line in infile:
		if "," not in line:
			continue
			
		parts = line.split(',')
		t0 = int(parts[0])
		t1 = int(parts[1])

		reply = (t0,t1)

		hasT0 = (t0 in tweets)
		hasT1 = (t1 in tweets)
		if not hasT0 and not hasT1:
			# create a new conversation
			conv = Conversation(reply)
			convs.add(conv)
			tweets[t0] = conv
			tweets[t1] = conv
			cnt += 1
		elif hasT0 and not hasT1:
			conv = tweets[t0]
			conv.addReply(t0,t1)
			tweets[t1] = conv
		elif hasT1 and not hasT0:
			conv = tweets[t1]
			conv.addReply(t0,t1)
			tweets[t0] = conv
		else:
			# must join 2 existing conversations
			conv1 = tweets[t0]
			conv2 = tweets[t1]
			#print "joining (%d,%d)"%(conv1.id,conv2.id)
			
			# skip conversations already joined - can this happen?
			if conv1 == conv2:
				print "conversations already joined!"
				continue
			
			# add all replies to the first conversation
			conv1.extend(conv2)

			# update dictionaries to all point to conv1
			tweets[t1] = conv1
			tweets[t0] = conv1
			# point all replies from the 2nd conv to conv1
			for subtweet in conv2.replies:
				tweets[subtweet[0]] = conv1
				tweets[subtweet[1]] = conv1

			if conv1 not in convs:
				print "conv1 not in convs (%d,%d)"%(conv1.id,conv2.id)
			if conv2 not in convs:
				print "conv2 not in convs (%d,%d)"%(conv1.id,conv2.id)
				print "total convs: %d/%d"%(len(convs),cnt)
			convs.remove(conv2)



print "# convs = %d"%(len(convs))
cnt = 0
longest = 0
widest = 0
deepest = 0
for c in convs:
	c.process()
	if len(c) > 2:
		cnt += 1
		longest = max(longest, len(c))
	widest = max(widest,c.breadth)
	deepest = max(deepest,c.depth)


print "# convs with more than 2 edges = %d"%(cnt)
print "largest conv has %d edges"%(longest)
print "widest: %d"%(widest)
print "deepest: %d"%(deepest)

reduced = [c for c in convs if \
	c.depth >= args.min_depth and \
	c.breadth >= args.min_width and \
	len(c.tweets) >= args.min_messages \
]

print "reduced size: %d"%(len(reduced))
print "-----------------------"

print "Connecting to db... (%s@%s %s)"%(args.dbuser,args.dbhost, args.dbname)
db = MySQLdb.connect(host=args.dbhost, user=args.dbuser, passwd=args.dbpass, db=args.dbname, charset='utf8', use_unicode=True)
cursor = db.cursor(cursors.SSCursor)

print "Adding %d conversations"%(len(reduced))
for c in reduced:
	vals = (c.breadth, c.depth, c.root['id'], len(c.tweets))
	#print vals
	cursor.execute(INSERT_CONVERSATION, (c.breadth, c.depth, c.root['id'], len(c.tweets)) )
	c.id = cursor.lastrowid
	for t in c.tweets.values():
		try:
			#print pretty(t)
			vals2 = (c.id, t["depth"], len(t["children"]), t['id'] )
			#print len(t['children'])
			#print "   ", vals2
			cursor.execute(UPDATE_TWEET_QUERY, vals2)
		except Exception,e:
			print "error: ", e

print "Running calculations..."
cursor.execute(UPDATE_CONVERSATION_WITH_CALCS)

cursor.close()
db.close()





