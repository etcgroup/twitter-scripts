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
from utils.helpers import *



class Conversation:
	def __init__(self, first):
		self.replies = []
		self.tweets = {}
		self.root = None
		if first is not None:
			self.addReply(first[0],first[1])
			self.id = first[1]
		else:
			self.id = random.randrange(0,100000,1)

	def addTweet(self,tweet):
		if tweet not in self.tweets:
			self.tweets[tweet] = {'parent': None, 'children': [], 'depth': 0}

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
		return "%d (len:%d)"%(self.id,len(self.replies))

#
#
# Check Args
#
#

# add args
parser = argparse.ArgumentParser()
parser.add_argument("infile", type=str, help="name of the input edge csv file")
parser.add_argument("outfile", type=str, help="name of the output edge csv file")
# parse args
args = parser.parse_args()


tweets = {}
convs = set()

cnt = 0
with open(args.infile, "rt") as infile:
	for line in infile:
		if "," in line:
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
			else:
				if hasT0 and not hasT1:
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
					if conv1 != conv2:
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
for c in convs:
	if len(c) > 2:
		cnt += 1
		longest = max(longest, len(c))
	if len(c) == 5:
		print pretty(c.replies)
		print pretty(c.tweets)

print "# convs with more than 2 edges = %d"%(cnt)
print "largest conv has %d edges"%(longest)

#with open(args.outfile, "wt") as outfile:
#	file_writer = csv.writer(outfile)
#	for c in convs:
#		if len(c) > 2:
#			for r in c.replies:
#				file_writer.writerow(r)

