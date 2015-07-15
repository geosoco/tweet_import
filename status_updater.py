#!/bin/python
from datetime import datetime


class StatusUpdater(object):

	def __init__(self, update_time = 5, count = 0, current_val = 0, total_val = 0, total_added = 0):
		self.update_time = update_time
		self.last_display_time = datetime.now()
		self.count = count
		self.current_val = current_val
		self.total_val = total_val
		self.total_added = 0
		self.total_files = 0
		self.current_file = 0

	def update(self, force=False):
		# update progress display if necessary
		cur_time = datetime.now()
		time_since_last_update = cur_time - self.last_display_time
		if force or time_since_last_update.total_seconds() > self.update_time:
			self.last_display_time = cur_time
			progress = self.current_val * 100.0 / self.total_val
			print "File %d / %d - parsed %d tweets (%2.2f%% finished). %d added."%(self.current_file+1, self.total_files, self.count, progress, self.total_added)
