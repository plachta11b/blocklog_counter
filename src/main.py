#!/usr/bin/env python3

###
### Count blocks modified by minecraft player
###

# https://www.python.org/dev/peps/pep-0008/#comments

import sys
import fcntl
import yaml
import time
import mysql.connector
from mysql.connector import errorcode
from os import path

from config import Config
from source_database import SourceDatabase
from destination_database import DestinationDatabase


###
###
###
def get_connection(database):

	connection_configuration = Config("./../config/config.yml").get_connection_configuration();

	# get database connection
	db_config = {
		"host": connection_configuration["host"],
		"user": connection_configuration["user"],
		"port": connection_configuration["port"],
		"password": connection_configuration["password"],
		"database": database,
		"raise_on_warnings": connection_configuration["raise_on_warnings"],
	}

	# do connection
	try:
		connection = mysql.connector.connect(**db_config)
	except mysql.connector.Error as error:
		if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
			exit("ERROR: Something is wrong with your user name or password")
		elif error.errno == errorcode.ER_BAD_DB_ERROR:
			exit("ERROR: Database '" + database + "' does not exists")
		else:
			exit(error)
	else:
		return connection;



###
###	get data from blocklog database
###
def get_data(database_source, blocklog_counter_database, world, number_of_items):

	# select items from id
	select_start = blocklog_counter_database.get_position(world)

	# select data to id -> prevent system overload
	select_end = select_start + number_of_items

	blocklog_database = SourceDatabase(get_connection(database_source), database_source, world)
	blocklog_database.fetch_data_from_to(select_start, select_end)
	database_row_count = blocklog_database.get_row_count()

	blocklog_counter_database.set_position(world, select_start + database_row_count)

	return blocklog_database



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

	main_start = time.time()

	if config.is_debug_mode_on():
		print("Start blocklog_counter with debugging on.")

	worlds = config.get_world_names()
	maximum_to_proccess = config.get_maximum_processed()

	block_counter_database = DestinationDatabase(get_connection(config.get_destination_database()), config)

	source_database = config.get_source_database()

	for world_name in worlds:
		database_data = get_data(source_database, block_counter_database, world_name, maximum_to_proccess)

		data = process_data(database_data)

		block_counter_database.save_data(world_name, data)

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
