### apt-get install python-pip #sudo
### pip install tendo #sudo
### pip install pyyaml #sudo
### crontab -e and set @reboot python /home/$user$/$script$.py #without sudo

# need install before import
import sys
import fcntl
import yaml
import mysql.connector
from mysql.connector import errorcode
# from tendo import singleton # not work now
from os import path

script_directory = path.dirname(path.realpath(__file__))
file_config = yaml.load(open(path.join(script_directory, "./../config/config.yml"), 'r'))

# run only single instance of program
# run_only_once = singleton.SingleInstance();

###
### prepare database
###
def prepare_database_structure(world):

	# do connection
	connection = get_connection("block_counter")

	# make cursor for fetch data
	cursor = connection.cursor()

	# do database schema if not exist
	cursor.execute('''
		CREATE TABLE IF NOT EXISTS ''' + world + ''' (
			`user` smallint(5) unsigned NOT NULL,
			`block` int(11) NOT NULL,
			`pick` int(11) NOT NULL DEFAULT 0,
			`put` int(11) NOT NULL DEFAULT 0,
			KEY `user` (`user`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8;
	''')

	cursor.execute('''
		CREATE TABLE IF NOT EXISTS `block_counter_position` (
			`world` varchar(64) COLLATE 'utf8_general_ci' NOT NULL,
			`block_id` int unsigned NOT NULL DEFAULT 0
		) ENGINE=InnoDB DEFAULT CHARSET=utf8;
	''')

	cursor.close()
	connection.commit()
	connection.close()

###
###
###
def get_connection(database):

	file_connection_config = file_config["connection"]

	# get database connection
	db_config = {
		"host": file_connection_config["host"],
		"user": file_connection_config["user"],
		"port": file_connection_config["port"],
		"password": file_connection_config["password"],
		"database": database,
		"raise_on_warnings": file_connection_config["raise_on_warnings"],
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
### get block id for process
###
def get_position(wolrd):

	# do connection
	connection = get_connection('block_counter')

	# make cursor for fetch data
	cursor = connection.cursor()

	cursor.execute('''
		SELECT block_id FROM `block_counter_position`
		WHERE world=%s
	''', [wolrd])

	# is there any row
	result = cursor.fetchone()

	if result is None:
		cursor.execute('''
			INSERT INTO `block_counter_position` (world, block_id)
			VALUES (%s, 0);
		''', [wolrd])

		result = 0
	else:
		cursor.execute('''
			SELECT block_id FROM `block_counter_position`
			WHERE world=%s
		''', [wolrd])

		result = cursor.fetchone()[0]

	cursor.close()
	connection.commit()
	connection.close()

	return result

###
### update how many items affected
###
def set_position(world, position):

	# do connection
	connection = get_connection("block_counter")

	# make cursor for fetch data
	cursor = connection.cursor()

	cursor.execute('''
		UPDATE `block_counter_position` SET block_id=%s
		WHERE world=%s
	''', [position, world])

	cursor.close()
	connection.commit()
	connection.close()


###
###	get data from blocklog database
###
def get_raw_data(world, number_of_items):

	# get database name
	database_name = file_config["database"]

	# do connection
	connection = get_connection(database_name)

	# make cursor for fetch data
	cursor = connection.cursor()

	# select items from id
	select_start = get_position(world)
	# select data to id -> prevent system overload
	select_end = select_start + number_of_items

	select_structure = ('''
		SELECT id, playerid, replaced, type, data
		FROM ''' + world + '''
		WHERE id BETWEEN %s AND %s
	''')

	cursor.execute(select_structure, [select_start, select_end - 1])

	if cursor.rowcount == 0:
		exit("no data to process")

	columns = cursor.description
	raw_data = []
	database_data = cursor.fetchall()
	for value in database_data:
		tmp = {}
		for (index,column) in enumerate(value):
			 tmp[columns[index][0]] = column
		raw_data.append(tmp)

	set_position(world, select_start + cursor.rowcount)

	cursor.close()
	connection.close()

	return raw_data


###
###	modify raw data
###
def process_raw_data(raw_data):

	# data storage
	pick = {};
	put = {};

	for (item) in raw_data:
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
### save into database
###
def save_data(world, data):

	def structure_exist(cursor, world, user, block):

		select = '''
			SELECT pick
			FROM ''' + world + '''
			WHERE user=%s AND block=%s
		'''

		cursor.execute(select, [user, block])

		return not cursor.fetchone() is None


	# do connection
	connection = get_connection('block_counter')

	# make cursor for fetch data
	cursor = connection.cursor()

	insert_item = ('''
		INSERT INTO ''' + world + '''
		(user, block, pick, put)
		VALUES (%s, %s, %s, %s)
	''')

	update_item = ('''
		UPDATE ''' + world + '''
		SET pick=pick+%s, put=put+%s
		WHERE user=%s AND block=%s
	''')

	pick = data["pick"]
	put = data["put"]

	for user in pick:
		for replaced in pick[user]:
			if structure_exist(cursor, world, user, replaced):
				cursor.execute(update_item, [pick[user][replaced], 0, user, replaced])
			else:
				cursor.execute(insert_item, [user, replaced, pick[user][replaced], 0])

	connection.commit()

	for user in put:
		for type in put[user]:
			if structure_exist(cursor, world, user, type):
				cursor.execute(update_item, [0, put[user][type], user, type])
			else:
				cursor.execute(insert_item, [user, type, put[user][type], 0])

	cursor.close()
	connection.commit()
	connection.close()

###
### main function
###
def main():
	world_name = file_config["get_worlds"]
	number_of_items = file_config["number_of_items"]

	for name in world_name:
		prepare_database_structure(name)

		raw_data = get_raw_data(name, number_of_items)

		data = process_raw_data(raw_data)

		save_data(name, data)

if __name__ == "__main__":

	pid_file = 'program.pid'
	fp = open(pid_file, 'w')

	try:
		fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
	except IOError:
		exit("ERROR: Another instance is running");

	main()
