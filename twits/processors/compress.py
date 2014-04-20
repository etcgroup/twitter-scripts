#! /usr/bin/env python
"""
A processing script that simply re-encodes 
parsed tweets as json and prints them out.
"""

import json
from tweet_processor import TweetProcessor

class Compressor(TweetProcessor):

    def __init__(self, *args, **kwargs):
        super(Compressor, self).__init__(*args, **kwargs)
        self.outfile = None
        
    def arguments(self, parser):
        parser.add_argument("outfile", type=str, help="name of the file to write tweets to")
        
    def setup(self):
        self.outfile = open(self.args.outfile, 'wb')
        
    def teardown(self):
        self.outfile.close()
        
    def process(self, tweet, raw_tweet):
        self.outfile.write(json.dumps(tweet))
        self.outfile.write('\n')

    def print_progress(self):
        if not self.outfile.closed:
            self.outfile.flush()
        
if __name__ == '__main__':
    processor = Compressor()
    processor.run()
