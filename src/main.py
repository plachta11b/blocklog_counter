#!/usr/bin/env python3

###
### Count blocks modified by minecraft player
###

# https://www.python.org/dev/peps/pep-0008/#comments

import sys
import fcntl
import yaml
import time
from os import path

from config import Config
from source_database import SourceDatabase
from destination_database import DestinationDatabase
from connection import Connection


###
###	get data from blocklog database
###
def get_data(blocklog_database, blocklog_counter_database, world, number_of_items):

	# select items from id
	select_start = blocklog_counter_database.get_position(world)

	# select data to id -> prevent system overload
	select_end = select_start + number_of_items

	source_data = blocklog_database.fetch_data_from_to(world, select_start, select_end)
	database_row_count = source_data.get_row_count()

	blocklog_counter_database.set_position(world, select_start + database_row_count)

	return source_data



###
###	compress raw data
###
def process_data(blocklog_database):

	# data storage
	pick = {};
	put = {};

	for item in blocklog_database.get_item():
		user = item["playerid"]
		replaced = item["replaced"]
		type = item["type"]

		if (replaced == 0):
			if not (user in put):
				put[user] = {}

			if not (type in put[user]):
				put[user][type] = 0

			put[user][type] += 1
		else:
			if not (user in pick):
				pick[user] = {}

			if not (replaced in pick[user]):
				pick[user][replaced] = 0

			pick[user][replaced] += 1

	# for user in pick:
	# 	for replaced in pick[user]:
	# 		print("player[{}] replace {} now {}x".format(user, replaced, pick[user][replaced]))

	return {"pick": pick, "put": put}


###
### main function
###
def main():

	config = Config("./../config/config.yml")

	# save time on start
	main_start = time.time()

	if config.is_debug_mode_on():
		print("Start blocklog_counter with debugging on.")

	# get configuration
	worlds = config.get_world_names()
	maximum_to_proccess = config.get_maximum_processed()

	# get database connection
	connection = Connection(config)
	block_counter_database = DestinationDatabase(connection.get_connection(config.get_destination_database()), config)
	blocklog_database = SourceDatabase(connection.get_connection(config.get_source_database()))

	for world_name in worlds:

		database_data = get_data(blocklog_database, block_counter_database, world_name, maximum_to_proccess)

		data = process_data(database_data)

		block_counter_database.save_data(world_name, data)

	# calculate process runtime
	main_end = time.time()
	main_elapsed = main_end - main_start

	if config.is_debug_mode_on():
		print("Time main running: {}".format(main_elapsed))

if __name__ == "__main__":

	# open file for locking
	pid_file = "program.pid"
	fp = open(pid_file, 'w')

	# try lock file to see if only one instance is running
	try:
		fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
	except IOError:
		exit("ERROR: Another instance is running");

	# no other instance is running -> start main function
	main()
