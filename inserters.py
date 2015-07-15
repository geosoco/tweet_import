#!/bin/python
import pymongo




class InserterBase(object):
	"""
	Inserter base class that does nothing
	"""

	def __init__(self, collection):
		self.collection = collection


	def addTweet(self, tweet):
		return 0

	def close(self):
		return 0



class BatchInserter(InserterBase):
	"""
	Inserter that will insert in batches.

	The default batch size to insert is 1000, but configurable through the size parameter in the constructor.

	NOTE: you must call close on this inserter, because the queue may not be full, and this will flush the remaining
	tweets.
	"""
	def __init__(self, collection, size=1000):
		super(BatchInserter, self).__init__(collection)
		self.batch_size = size
		self.tweets = []

	def doInsert(self):
		try: 
			self.collection.insert(self.tweets, ordered=True)
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



class SingleInserter(InserterBase):
	"""
	Simple inserter that inserts one tweet at a time
	"""

	def addTweet(self, tweet):
		try:
			self.collection.insert_one(tweet)

			return 1
		except Exception, e:
			print "Failed to insert tweets: ", e
			return 0




class SingleExistenceCheckingInserter(InserterBase):
	"""
	Inserts one tweet at a time, but first checks to make sure another tweet with that id has already been
	inserted.
	"""

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
			self.collection.insert_one(tweet)
			return 1
		except Exception, e:
			print "Failed to insert tweets: ", e
			return 0




#
# factory function
#
def get_inserter(batchsize, check = False):
	"""
	Factory function to get an inserter based on the parameters passed in

	batchsize is the number of tweets to insert at one time, and check
	checks if the tweet with that id has already been inserted (and ignores it)
	"""
	inserter_obj = None

	if batchsize > 1:

		# error if the user also specified check
		if check == True:
			print "Error: can't check existence for batch inserts."
			return None

		inserter_obj = BatchInserter(collection, batchsize)

	else:
		if check == False:
			inserter_obj = SingleInserter(collection)
		else:
			inserter_obj = SingleExistenceCheckingInserter(collection)	

	return inserter_obj


