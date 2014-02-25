"""
Parses a list of twitter json messages (separated by commas) such
as would be output by stream.py.

To use this, you must create a class that extends TweetProcessor.
You could create a file like this:

---------------
# twitter-scripts/processors/myprocessor.py
from tweet_processor import TweetProcessor

class MyProcessor(TweetProcessor):
   def process(self, tweet, raw_tweet):
      pass # your stuff goes here

processor = MyProcessor()
processor.run()
--------------
"""

import time
import argparse
import os
import json

from utils.helpers import *

# the chunk size for reading in the file
PARSE_SIZE = 8 * 1024 * 1024


class TweetProcessor(object):
    """
    Base class for a generic tweet processor.
    Read raw json tweets from a file and run them through
    the process() function.
    """

    def __init__(self):
        self.args = None
        self.tweet_read_time = 0
        self.tweet_process_time = 0
        self.tweet_parse_time = 0
        self.start_time = 0
        
        
    def arguments(self, parser):
        """Add any needed arguments to the argparse parser"""
        pass

        
    def setup(self):
        """
        Perform any setup before processing begins.
        self.args will contain the arguments from argparse.
        """
        pass
        
        
    def process(self, tweet, raw_tweet):
        """Process the tweet. raw_tweet is the unparsed json string"""
        pass
        
        
    def teardown(self):
        """Perform any actions before the program ends."""
        pass
        
        
    def print_progress(self):
        """If there are any progress indicators, print them here."""
        pass

        
    def _print_progress(self):
        print "--- Timing {0:0.3f}s (total) ---".format(time.time() - self.start_time)
        
        print "  Totals: {0:0.3f}s (read) {1:0.3f}s (parse) {2:0.3f}s (process)".format(
            self.tweet_read_time, self.tweet_parse_time, self.tweet_process_time
        )
        
        self.print_progress()
        
    def _configure_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--limit", type=int, help="maximum number of tweets to process", default=None)
        parser.add_argument("tweetsfile", type=str, help="name of the file containing tweets")
        
        self.arguments(parser)
        self.args = parser.parse_args()
    
    def run(self):
        
        self._configure_args()
        
        self.setup()
        
        print "Parsing %s..."%(self.args.tweetsfile)
        if self.args.limit:
            print "up to %d tweets..." % self.args.limit

        with open(self.args.tweetsfile, "rt") as infile:
            self.start_time = time.time()
        
            # grab file size
            infile.seek(0,os.SEEK_END)
            filesize = infile.tell()
            infile.seek(0,os.SEEK_SET)

            # start our read loop with valid data
            raw = ''
            tweet_start_found = False
            start = 0
            tweet_count = 0
            last_parse_position = 0
            for line in infile:

                if line[0] == '{':
                    # start of tweet
                    tweet_start_found = True
                    start = time.time()
                    raw = ''
                    raw += line
                elif line[0:2] == '},' and tweet_start_found == True:
                    # end of tweet
                    raw += line[0]
                    tweet_start_found = False
                    self.tweet_read_time += time.time() - start

                    start = time.time()
                    tweet = json.loads(raw)
                    self.tweet_parse_time += time.time() - start;

		    # make sure it is a tweet
		    if 'user' in tweet:
			    start = time.time()
			    self.process(tweet, raw)
			    self.tweet_process_time += time.time() - start
			    tweet_count += 1

                elif tweet_start_found == True:
                    # some line in the middle
                    raw += line

                cur_pos = infile.tell()
                if (cur_pos - last_parse_position) > PARSE_SIZE:
                    last_parse_position = cur_pos
                    pct_done = (float(cur_pos) * 100.0 / float(filesize))
                    print "===================="
                    print "%f%% complete..."%(pct_done)
                    self._print_progress()

                if self.args.limit and self.args.limit < tweet_count:
                    break
    
            self.teardown()
            self._print_progress()
            
        print "Done processing %s..."%(self.args.tweetsfile)
        


if __name__ == '__main__':
	processor = TweetProcessor()
	processor.run()
