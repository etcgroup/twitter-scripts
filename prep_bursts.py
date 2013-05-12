#
# prep_bursts
#
# by mjbrooks
#
# modified 5-12-2013 by soco for database additions
#

import os
import sys
import simplejson, csv
from datetime import datetime
import calendar
import argparse
import re
import MySQLdb
from MySQLdb import cursors
import getpass


DATETIME_FORMAT = "%Y%m%d %H:%M:%S +0000"
INSERT_BURSTS = """insert INTO burst_keywords 
(mid_point, window_size, before_total_words, after_total_words, term, before_count, after_count, count_delta, 
    count_percent_delta, before_rate, after_rate, rate_delta, rate_percent_delta, before_relevance, after_relevance, 
    relevance_delta, relevance_percent_delta) 
VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

DBHOST = None
DBUSER = 'root'
DBPASS = ''
DBNAME = 'twitter'
DBPORT = 3306

termRX = r"(?P<term>[@#\w ]+)"
durationRX = r"(?P<duration>\d+)"
dateTimeRX = r"\d+ \d\d:\d\d:\d\d \+\d+"
floatingPoint = r"\d*\.?\d*(E-?\d+)?"
aggregatesRX = r"(?P<termCountDelta>\d+),(?P<relevanceDelta>" + floatingPoint + "),(?P<arrivalRateDelta>" + floatingPoint + "),(?P<countPercentDelta>" + floatingPoint + "),(?P<ratePercentDelta>" + floatingPoint + "),(?P<relevancePercentDelta>" + floatingPoint + ")"
binOneRX = r"\[(?P<dateTimeOne>" + dateTimeRX + r"),(?P<termCountOne>\d+),(?P<totalWordsOne>\d+),(?P<arrivalRateOne>" + floatingPoint + "),(?P<relevanceOne>" + floatingPoint + ")\]"
binTwoRX = r"\[(?P<dateTimeTwo>" + dateTimeRX + r"),(?P<termCountTwo>\d+),(?P<totalWordsTwo>\d+),(?P<arrivalRateTwo>" + floatingPoint + "),(?P<relevanceTwo>" + floatingPoint + ")\]"
lineRX = termRX + "\t" + r"\{" + durationRX + "," + binOneRX + binTwoRX + "," + aggregatesRX + r"\}\s+\d"
# print lineRX
# exit()
lineParse = re.compile(lineRX)
dbRowsToInsert = []

class Burst(object):
    
    def __init__(self, match, rawLine):
        self.rawLine = rawLine
        
        self.term = match.group('term')
        self.windowSize = match.group('duration')
        self.midPoint = self._parseTime(match.group('dateTimeTwo'))
        
        self.beforeCount = match.group('termCountOne')
        self.afterCount = match.group('termCountTwo')
        self.countDelta = match.group('termCountDelta')
        self.countPercentDelta = self._science(match.group('countPercentDelta'))
        
        self.beforeArrivalRate = self._science(match.group('arrivalRateOne'))
        self.afterArrivalRate = self._science(match.group('arrivalRateTwo'))
        self.arrivalRateDelta = self._science(match.group('arrivalRateDelta'))
        self.ratePercentDelta = match.group('ratePercentDelta')
        
        self.beforeRelevance = self._science(match.group('relevanceOne'))
        self.afterRelevance = self._science(match.group('relevanceTwo'))
        self.relevanceDelta = self._science(match.group('relevanceDelta'))
        self.relevancePercentDelta = self._science(match.group('relevancePercentDelta'))
        
        self.beforeTotalWords = match.group('totalWordsOne')
        self.afterTotalWords = match.group('totalWordsTwo')

    # Parses a date in 20130203 00:20:00 +0000 format
    def _parseTime(self, dateTime):
        return datetime.strptime(dateTime, DATETIME_FORMAT)
    
    def _formatTime(self, dateTime):
        return calendar.timegm(dateTime.utctimetuple())
    
    # Parses a number that may be in scientific notation
    # and formats it with high precision, NOT in scientific
    # notation.
    def _science(self, floatingPointString):
        return "{0:0.16f}".format(float(floatingPointString))
    
    @staticmethod
    def csvHeader(writer):
        writer.writerow([
            'mid_point', 
            'window_size',
            'before_total_words',
            'after_total_words',
            'term',
            'before_count',
            'after_count', 
            'count_delta',
            'count_percent_delta', 
            'before_rate', 
            'after_rate', 
            'rate_delta',
            'rate_percent_delta',
            'before_relevance',
            'after_relevance',
            'relevance_delta',
            'relevance_percent_delta'
        ])
        
    def csv(self, writer):
        writer.writerow([
            self._formatTime(self.midPoint),
            self.windowSize,
            self.beforeTotalWords,
            self.afterTotalWords,
            self.term,
            self.beforeCount, 
            self.afterCount, 
            self.countDelta,
            self.countPercentDelta, 
            self.beforeArrivalRate, 
            self.afterArrivalRate, 
            self.arrivalRateDelta,
            self.ratePercentDelta,
            self.beforeRelevance,
            self.afterRelevance,
            self.relevanceDelta,
            self.relevancePercentDelta
        ])

        
def read_bursts(file, numLines):
    printout("Processing %s" %(file.name))
    # something like this:
    # watt	{1200,[20130203 00:20:00 +0000,5,0.0042][20130203 00:40:00 +0000,821,0.6842],816,0.68,16320.0,16320.0}	1
    lineNumber = 0
    
    bursts = []
    for line in file:
        lineNumber += 1
        
        if lineNumber > numLines:
            break;
        
        # skip blank lines
        if not line.strip():
            continue;
        
        match = lineParse.match(line)
        if not match:
            printout("No match on line %s"%(lineNumber))
            printout(">>>%s" %(line))
            continue
        
        burst = Burst(match, line)
        bursts.append(burst)
    
    return bursts



def buffer_burst(burst):
    global dbRowsToInsert

    row = (burst._formatTime(burst.midPoint),
        burst.windowSize,
        burst.beforeTotalWords,
        burst.afterTotalWords,
        burst.term,
        burst.beforeCount, 
        burst.afterCount, 
        burst.countDelta,
        burst.countPercentDelta, 
        burst.beforeArrivalRate, 
        burst.afterArrivalRate, 
        burst.arrivalRateDelta,
        burst.ratePercentDelta,
        burst.beforeRelevance,
        burst.afterRelevance,
        burst.relevanceDelta,
        burst.relevancePercentDelta)
    dbRowsToInsert.append(row)

def dbflush(db, cursor):
    global dbRowsToInsert

    try:
        cursor.executemany(INSERT_BURSTS, dbRowsToInsert)
    except Exception, e:
        print "Error inserting bursts: ", e

    dbRowsToInsert = []


       
def printout(message):
    print >> sys.stderr, message
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("dir", type=str, help="directory containing burst window files")
    parser.add_argument("--out", type=str, help="output csv file")
    parser.add_argument('-N', help="top N terms from each file will be collected", default=10)
    parser.add_argument("--dbhost", help="Database host name")
    parser.add_argument("--dbuser", help="Database user name", default=DBUSER)
    parser.add_argument("--dbname", help="Database name", default=DBNAME)
    parser.add_argument("--dbport", type=int, help="Database port number", default=DBPORT)
    parser.add_argument("-p", "--dbpass", help="name of the database user", action='store_true')    
    args = parser.parse_args()
    
    # grab db password
    if args.dbpass:
        args.dbpass = getpass.getpass('enter database password: ')
    else:
        args.dbpass = ''

    # build our list of files to process
    printout('Searching for files in %s...' %(args.dir))
    files = list()
    for file in os.listdir(args.dir):
        path = os.path.join(args.dir, file)
        if os.path.isfile(path):
            files.append(path)
            
    printout('Found %s files.' %(len(files)))
    
    # write the CSV header
    writer = None 
    if args.out is not None:
        with open(args.out, 'wb') as outfile:
            writer = csv.writer(outfile)
            Burst.csvHeader(writer)


    # start db connection
    db = None
    cursor = None
    if args.dbhost is not None:
        print "Connecting to db... (%s@%s:%d %s)"%(args.dbuser,args.dbhost,args.dbport, args.dbname)
        db = MySQLdb.connect(host=args.dbhost, user=args.dbuser, passwd=args.dbpass, db=args.dbname, port=args.dbport, charset='utf8', use_unicode=True)
        cursor = db.cursor(cursors.SSCursor)

    
    for file in files:
        with open(file, "rU") as infile:
            bursts = read_bursts(infile, args.N)
            for burst in bursts:
                # write CSV if necessary
                if args.out is not None:
                    burst.csv(writer)

                # insert into DB if necessary
                if args.dbhost is not None:
                    buffer_burst(burst)

            dbflush(db,cursor)


                    