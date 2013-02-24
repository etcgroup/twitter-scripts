# twitter scripts for processing twitter data

This is a collection of python and sql scripts to capture twitter data from a series of usernames, hashtags, or keywords and
inserts them into a database. Various additional scripts help process the data for conversations and to grab sentiment data
using Sentiment140.

## fetchmissing.py

This script grabs any tweets that are referenced as a reply in the database, but do not actually exist in the database.

## grab_replies.py

(badly named). This grabs a list of all replies and writes a csv file of all the directed pairs. 


## parse.py

parses the JSON dump files and inserts/updates the database


## separate.py

(no longer needed?) separates the JSON dump into invidividual messages.


## stream.py

Script used to grab the tweets. It requires two input files, one which contains the developer keys (see credentials-example.txt) and one that contains the keyword list used to capture the data.

## twitter_schema.sql

database schema


## updatesentiment.py

runs through all messages and adds sentiment data from sentiment140


