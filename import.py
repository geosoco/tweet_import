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
import email.utils
from datetime import datetime
from pymongo import MongoClient


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




class Inserter(object):

	def __init__(self, collection):
		self.collection = collection


	def addTweet(self, tweet):
		return 0

	def close(self):
		return 0



class BatchInserter(Inserter):

	def __init__(self, collection, size=1000):
		super(BatchInserter, self).__init__(collection)
		self.batch_size = size
		self.tweets = []

	def doInsert(self):
		try: 
			self.collection.insert(self.tweets)
		except Exception, e:
			print "Failed to insert tweets: ", e

		# truncate the array
		self.tweets = []



	def addTweet(self, tweet):
		self.tweets.append(tweet)
		#print "%d tweets / %d tweets"%(len(self.tweets), self.batch_size)
		if len(self.tweets) >= self.batch_size:
			self.doInsert()
			return self.batch_size
		
		return 0

	def close(self):
		print "closing..."
		num_tweets = len(self.tweets)
		if num_tweets > 0:
			self.doInsert()
		return num_tweets



class SingleInserter(Inserter):

	def addTweet(self, tweet):
		try:
			self.collection.insert(tweet)

			return 1
		except Exception, e:
			print "Failed to insert tweets: ", e
			return 0




class SingleExistenceCheckingInserter(Inserter):

	def addTweet(self, tweet):

		# try to find the tweet
		try:
			tweet_id = long(tweet["id"])
			cursor = self.collection.find({ "id": tweet_id}, limit=1)
			# do nothing if tweet exists already
			if cursor.count() > 0:
				print "Skipping tweet with id: %s"%(str(tweet_id))
				cursor.close()
				return 0

			cursor.close()

		except Exception, e:
			print "Query failed finding tweet of id"

		# insert the tweet
		try:
			self.collection.insert(tweet)
			return 1
		except Exception, e:
			print "Failed to insert tweets: ", e
			return 0



class StatusUpdater(object):

	def __init__(self, update_time = 5, count = 0, current_val = 0, total_val = 0, total_added = 0):
		self.update_time = update_time
		self.last_display_time = datetime.now()
		self.count = count
		self.current_val = current_val
		self.total_val = total_val
		self.total_added = 0

	def update(self, force=False):
		# update progress display if necessary
		cur_time = datetime.now()
		time_since_last_update = cur_time - self.last_display_time
		if force or time_since_last_update.total_seconds() > self.update_time:
			self.last_display_time = cur_time
			progress = self.current_val * 100.0 / self.total_val
			print "parsed %d tweets (%2.2f%% finished). %d added."%(self.count, progress, self.total_added)

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
parser.add_argument('-f', '--filename', help="input file")
parser.add_argument('-e', '--encoding', default="utf-8", help="json file encoding (default is utf-8)")
parser.add_argument('-b', '--batchsize', default=1000, type=int, help="batch insert size")
parser.add_argument('-c', '--check', dest="check", action="store_true", help="check if tweet exists before inserting")
parser.add_argument('-r', '--no_retweets', dest="no_retweets", action="store_true", help="do not add embedded retweets")
parser.set_defaults(feature=False)


args = parser.parse_args()


#
# tweet id set
#
added_tweet_ids = set()


#
# connect to the database
#
try:
	client = MongoClient(args.host)
	db = client[args.database]
	collection = db[args.collection]
except Exception, e:
	print "failed to connect to '%s' and open db (%s) or collection (%s): %s"%(args.database, args.collection, e)
	quit()


#
# retweet dictionary and inserter
#
retweet_dict = {}
retweet_inserter = SingleExistenceCheckingInserter(collection)

#
# create the inserter
#
inserter = None

if args.batchsize > 1:

	# error if the user also specified check
	if args.check == True:
		print "Error: can't check existence for batch inserts."
		quit()

	inserter = BatchInserter(collection, args.batchsize)

else:
	if args.check == False:
		inserter = SingleInserter(collection)
	else:
		inserter = SingleExistenceCheckingInserter(collection)

#
# open the file
#
print "no retweets: %s"%(args.no_retweets)

print "opening \"%s\" with encoding \"%s\""%(args.filename, args.encoding)
with open(args.filename, "r") as f:

	# get file length
	f.seek(0, os.SEEK_END)
	file_length = f.tell()
	f.seek(0, os.SEEK_SET)

	# store the current time so we know when to update
	status_updater = StatusUpdater(total_val=file_length)

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
			print "tweet %d already added"%(tweet_id)
			continue

		added_tweet_ids.add(tweet_id)

		#process tweet		
		tweet['created_ts'] = convertRFC822ToDateTime(tweet['created_at'])
		tweet['user']['created_ts'] = convertRFC822ToDateTime(tweet['user']['created_at'])




		# process retweeted_status
		retweet_id = None
		if 'retweeted_status' in tweet:
			retweet_id = tweet['retweeted_status']['id']
			tweet['retweeted_status']['created_ts'] = convertRFC822ToDateTime(tweet['retweeted_status']['created_at'])
			tweet['retweeted_status']['user']['created_ts'] = convertRFC822ToDateTime(tweet['retweeted_status']['user']['created_at'])

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

		# we finished processing one
		status_updater.count += tweet_inc


		# update progress display if necessary
		status_updater.current_val = f.tell()
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

	for k,v in retweet_dict.iteritems():
		tweet_inc = retweet_inserter.addTweet(v['tweet'])
		status_updater.total_added += tweet_inc

		# we finished processing one
		status_updater.count += tweet_inc
		status_updater.current_val += 1
		status_updater.update()


	status_updater.total_added += retweet_inserter.close()
	status_updater.update(True)


# shutdown our handles and connections
f.close()
client.close()

