#
#
#
#
#
# Note: gnip tweets don't have timestamp_ms field, only the created_at. 
# Livestreams do have the timestamp_ms field which offers slightly greater granularity
#
#

import os
import io
import sys
import simplejson as json
import argparse
import codecs
import glob
import email.utils
from datetime import datetime
import pymongo

from inserters import *
from status_updater import *


#
# convert an RFC822 date to a datetime
# borrowed and modified from: https://gist.github.com/robertklep/2928188
#
# original used fromtimestamp instead of utcfromtimestamp, which converts it to local time.
# currently, all of the tweets seem to just use utc, so we can ignore the timestamp (though
# it's unclear if mktime_tz actually utilizes the info or just ignores it. )
#
def convertRFC822ToDateTime(rfc822string):
	"""
		convert an RFC822 date to a datetime
	"""
	return datetime.utcfromtimestamp(email.utils.mktime_tz(email.utils.parsedate_tz(rfc822string)))








#
#
# program arguments
#
#
parser = argparse.ArgumentParser(description='whatevs')
parser.add_argument('host', help='host')
parser.add_argument('database', help='database name')
parser.add_argument('collection', help="collection name")
parser.add_argument('-l', '--limit', help="limit", type=int, default=0)
parser.add_argument('-o', '--output', help="outfile")
parser.add_argument('-f', '--filename', help="input files")
parser.add_argument('-e', '--encoding', default="utf-8", help="json file encoding (default is utf-8)")
parser.add_argument('-b', '--batchsize', default=1000, type=int, help="batch insert size")
parser.add_argument('-c', '--check', dest="check", action="store_true", help="check if tweet exists before inserting")
parser.add_argument('-r', '--no_retweets', dest="no_retweets", action="store_true", help="do not add embedded retweets")
parser.add_argument('--no_index', dest="no_index", action="store_true", help="do not create an index for tweet ids")
parser.set_defaults(feature=False)


args = parser.parse_args()




#
# tweet id set
#
added_tweet_ids = {}
TWEET_ORIGINAL = 1
TWEET_RETWEET = 2


#
# connect to the database
#
try:
	client = pymongo.MongoClient(args.host)
	db = client[args.database]
	collection = db[args.collection]
except Exception, e:
	print "failed to connect to '%s' and open db (%s) or collection (%s): %s"%(args.database, args.collection, e)
	quit()


print "no retweets: %s"%(args.no_retweets)


#
# create indexes
#
if not args.no_index:
	collection.create_index( [("id", pymongo.ASCENDING )])

#
# retweet dictionary and inserter
#
retweet_dict = {}
#retweet_inserter = SingleExistenceCheckingInserter(collection)

#
# create the inserter
#
inserter = get_inserter(collection, args.batchsize, args.check)
if inserter is None:
	print "Couldn't create database inserter"
	quit()

#  create status updater
status_updater = StatusUpdater()

# build file list
file_list = sorted(glob.glob(args.filename))
file_count = len(file_list)
status_updater.total_files = file_count

#
# step through each file
#

for filename_index in range(file_count):

	filename = file_list[filename_index]
	print "opening \"%s\" with encoding \"%s\" (%d of %d)"%(filename, args.encoding, filename_index+1, file_count)
	with open(filename, "r") as f:

		# get file length
		f.seek(0, os.SEEK_END)
		file_length = f.tell()
		f.seek(0, os.SEEK_SET)

		status_updater.current_file = filename_index 

		# update status_updater
		status_updater.total_val = file_length

		# tweets are expected on each line
		for rawline in f:

			# check for empty lines
			rawline = rawline.strip()
			if not rawline:
				continue

			line = codecs.decode(rawline, args.encoding)

			# convert it to json
			try:
				tweet = None

				#print "parsing ----"
				#print line
				#print "\n"*4
				tweet = json.loads(line)

			except Exception, e:
				print "failed to parse json: ", e
				print line

			# continue if the tweet failed
			if tweet is None:
				continue

			# see if this is a gnip info message, and skip if it is
			if 'info' in tweet and 'message' in tweet['info']:
				# print "info tweet", repr(tweet)
				continue

			# make sure it's a tweet
			if not 'text' in tweet or not 'created_at' in tweet or not 'user' in tweet:
				print "line is not a recognized tweet..."
				print "> ", line
				print "----------"
				continue

			# check to see if it's been processed, if not, add it to set
			tweet_id = tweet['id']
			if tweet_id in added_tweet_ids:
				print "tweet %d already added [%d]"%(tweet_id, added_tweet_ids[tweet_id])
				continue

			added_tweet_ids[tweet_id] = TWEET_ORIGINAL

			#process tweet		
			tweet['created_ts'] = convertRFC822ToDateTime(tweet['created_at'])
			tweet['user']['created_ts'] = convertRFC822ToDateTime(tweet['user']['created_at'])




			# process retweeted_status
			retweet_id = None
			if 'retweeted_status' in tweet:
				retweet_id = tweet['retweeted_status']['id']
				tweet['retweeted_status']['created_ts'] = convertRFC822ToDateTime(tweet['retweeted_status']['created_at'])
				tweet['retweeted_status']['user']['created_ts'] = convertRFC822ToDateTime(tweet['retweeted_status']['user']['created_at'])
				tweet['retweeted_status']['source_tweet'] = tweet_id

			#print tweet['created_ts']
			#print "\n"*4


			#print json.dumps(tweet)
			#print "\n"*4

			tweet_inc = inserter.addTweet(tweet)
			status_updater.total_added += tweet_inc

			# handle retweets if flag says so
			if args.no_retweets == False:
				# see if this tweet is in our retweet set
				if tweet_id in retweet_dict:
					# if so, remove it from the retweet dict to save time
					del retweet_dict[tweet_id]

				# if there's a retweet, try to add it
				if retweet_id is not None:
					if retweet_id not in added_tweet_ids:
						# try to keep the most recent tweet in the dict
						if retweet_id not in retweet_dict or retweet_dict[retweet_id]['ts'] < tweet['created_ts']:
							# add the most recent one in
							retweet_dict[retweet_id] = {
								'ts': tweet['created_ts'],
								'tweet': tweet['retweeted_status']
							}

							# add it to the added_tweet list too
							added_tweet_ids[retweet_id] = TWEET_RETWEET

			# we finished processing one
			status_updater.count += tweet_inc


			# update progress display if necessary
			#status_updater.current_val = f.tell()
			status_updater.current_val = os.lseek(f.fileno(), 0, os.SEEK_CUR)
			status_updater.update()


			if args.limit > 0 and status_updater.count >= args.limit:
				break



status_updater.total_added += inserter.close()
status_updater.update(True)


# now add retweets if necessary
num_retweets = len(retweet_dict)
if args.no_retweets == False and num_retweets > 0:
	print "Adding %d retweets"%(num_retweets)
	status_updater.total_val = num_retweets
	status_updater.current_val = 0
	status_updater.count = 0
	status_updater.update(True)

	inserter = get_inserter(args.batchsize, args.check)

	for k,v in retweet_dict.iteritems():
		tweet_inc = inserter.addTweet(v['tweet'])
		status_updater.total_added += tweet_inc

		# we finished processing one
		status_updater.count += tweet_inc
		status_updater.current_val += 1
		status_updater.update()


	status_updater.total_added += inserter.close()
	status_updater.update(True)


# shutdown our handles and connections
f.close()
client.close()

