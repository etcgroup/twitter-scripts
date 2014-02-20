"""
Parses a list of twitter json messages (separated by commas) such
as would be output by stream.py.

The "feeling" is extracted from tweets containing a "feeling marker",
as described here: https://github.com/lauren/we-still-feel-fine

"""

import csv
import re
import sys
from datetime import timedelta, datetime
from os import path

import nltk

from ..utils.twitter import parse_twitter_date
from tweet_processor import TweetProcessor


FEELINGS_FILE = path.join(path.dirname(__file__), 'feelings.txt')

FEELINGS_INDICATORS = ['i feel', 'you feel', 'we feel']
INDICATOR_REG_TEMPLATE = (r"([^\w]|^)"   # something that is not wordy
                          r"%s"          # the term/phrase
                          r"([^\w]|$)")  # another non-wordy bit

WINDOW_AFTER = 5
WINDOW_BEFORE = 2


class FeelingsExtractor(TweetProcessor):

    def __init__(self):
        super(FeelingsExtractor, self).__init__()
        
        self.bin_delta = None
        self.column_map = None
        self.feelings = None
        self.feelings_dict = None
        self.indicators = None

        self.bins_created = 0
        self.bin_delta = None
        self.current_bin = None
        self.current_bin_end = None

        self.tweet_count = 0
        self.non_rt_count = 0
        self.feels_count = 0
        self.valid_count = 0
        
    def arguments(self, parser):
        """Add any needed arguments to the argparse parser"""
        parser.add_argument("--binseconds", type=int, help="bin size, in number of seconds", default=60)
        parser.add_argument("outputfile", type=str, help="the csv file to write to", default="feelings_out.csv")
        
    def setup(self):
        """
        Perform any setup before processing begins.
        self.args will contain the arguments from argparse.
        """
        
        self.bin_delta = timedelta(seconds=self.args.binseconds)

        self.column_map = {
            "_bin": 0,
            "_time": 1,
            "_total_tweets": 2,
            "_total_non_rt": 3,
            "_total_feels": 4,            
            }

        colidx = 5

        self.indicators = []
        for ind in FEELINGS_INDICATORS:
            exp = INDICATOR_REG_TEMPLATE % re.escape(ind)
            self.indicators.append((ind, re.compile(exp, re.IGNORECASE & re.MULTILINE)))

            self.column_map[ind] = colidx
            colidx += 1


        # read in the tab-separated feelings file
        self.feelings = []
        self.feelings_dict = {}
        with open(FEELINGS_FILE, 'rb') as feelingsfile:
            reader = csv.reader(feelingsfile, delimiter='\t')
            for row in reader:
                f = row[0]
                self.feelings.append(f)
                self.feelings_dict[f] = True
                self.column_map[f] = colidx
                colidx += 1


        self.writer = csv.writer(open(self.args.outputfile, 'wb'))

        header = sorted(self.column_map.iterkeys(), key=lambda k: self.column_map[k])
        self.writer.writerow(header)

        
    def process(self, tweet, raw_tweet):
        """Process the tweet. raw_tweet is the unparsed json string"""

        
        # see how we're doing on bins
        created_at = tweet_created_at = parse_twitter_date(tweet['created_at'])
        self.update_bin(created_at)

        self.tweet_count += 1
        self.inc_bin_val("_total_tweets")

        # skip retweets
        if 'retweeted_status' in tweet:
            return

        self.non_rt_count += 1
        self.inc_bin_val("_total_non_rt")

        # does it contain a feeling indicator?
        firsthalf = None
        secondhalf = None
        for ind, reg in self.indicators:
            segments = reg.split(tweet['text'].lower(), maxsplit=1)
            if len(segments) == 4:
                firsthalf = segments[0]
                secondhalf = segments[3]
                self.inc_bin_val(ind)
                break
            if len(segments) > 1:
                raise Exception("Unexpected result from split: %s" %(str(segments)))
            
        if firsthalf is None:
            return

        self.feels_count += 1
        self.inc_bin_val("_total_feels")

        # select the window of possible words
        before_tokens = nltk.word_tokenize(firsthalf)[-WINDOW_BEFORE:]
        after_tokens = nltk.word_tokenize(secondhalf)[:WINDOW_AFTER]
        
        valid = False
        # see if any of them are in the word set
        for token in before_tokens + after_tokens:
            if token in self.feelings_dict:
                self.inc_bin_val(token)
                valid = True

        if valid:
            self.valid_count += 1


    def update_bin(self, current_dt):
        
        if self.current_bin is None or current_dt > self.current_bin_end:
            
            if self.current_bin_end:
                next_bin_start = self.current_bin_end
            else:
                next_bin_start = current_dt

            # submit the current bin if there is one
            self.emit_bin()

            # init a new bin
            self.current_bin = [0] * len(self.column_map)
            self.set_bin_val("_bin", self.bins_created)
            self.bins_created += 1
            self.set_bin_val("_time", str(next_bin_start))
            
            self.current_bin_end = next_bin_start + self.bin_delta

    def emit_bin(self):
        if self.current_bin:
            self.writer.writerow(self.current_bin)


    def set_bin_val(self, name, val):
        self.current_bin[self.column_map[name]] = val
        
        
    def inc_bin_val(self, name):
        self.current_bin[self.column_map[name]] += 1

        
    def print_progress(self):

        print "--- Counts ---"
        print "          total:", self.tweet_count, "tweets"
        print "         non RT:", self.non_rt_count, "tweets"
        print "feels indicated:", self.feels_count, 'tweets'
        print "    valid feels:", self.valid_count, "tweets"
        print "    bins output:", self.bins_created, "bins"


if __name__ == '__main__':
	processor = FeelingsExtractor()
	processor.run()
