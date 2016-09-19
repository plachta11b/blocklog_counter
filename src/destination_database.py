from connection import Connection

# SQL WTF
# Backticks (`) are to be used for table and column identifiers
# Single quotes (') should be used for string values

class DestinationDatabase:

	connection = None
	cursor = None
	config = None
	table_prefix = ""

	def __init__(self, config):
		self.connection = Connection(config).get_connection("destination")
		self.cursor = self.connection.cursor()
		self.config = config
		self.table_prefix = config.get_table_prefix()

		worlds = config.get_world_names()

		for world_name in worlds:
			self.prepare_database_structure(world_name)


	###
	### prepare database
	###
	def prepare_database_structure(self, world):

		table_world = "`{}{}`".format(self.table_prefix, world)
		table_position = "`{}bcposition`".format(self.table_prefix)

		# do database schema if not exist
		self.cursor.execute('''
			CREATE TABLE IF NOT EXISTS ''' + table_world + ''' (
				`user` smallint(5) unsigned NOT NULL,
				`block` int(11) NOT NULL,
				`pick` int(11) NOT NULL DEFAULT 0,
				`put` int(11) NOT NULL DEFAULT 0,
				primary key (user, block)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		''')
				# -- KEY `user` (`user`)

		self.cursor.execute('''
			CREATE TABLE IF NOT EXISTS ''' + table_position + ''' (
				`world` varchar(64) COLLATE 'utf8_general_ci' NOT NULL,
				`last_block_id` int unsigned NOT NULL DEFAULT 0
			) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		''')

		self.connection.commit()



	###
	### get block id for process
	###
	def get_position(self, world):

		table = self.table_prefix + "bcposition"

		self.cursor.execute('''
			SELECT last_block_id
			FROM `{}`
			WHERE world='{}'
		'''.format(table, world))

		# is there any row
		result = self.cursor.fetchone()

		if result is None:
			self.cursor.execute('''
				INSERT INTO `{}` (world, last_block_id)
				VALUES ('{}', 0);
			'''.format(table, world))

			result = 0
		else:
			self.cursor.execute('''
				SELECT last_block_id FROM `{}`
				WHERE world='{}'
			'''.format(table, world))

			result = self.cursor.fetchone()[0]

		self.connection.commit()

		return result



	###
	### update how many items affected
	###
	def set_position(self, world, position):

		table = self.table_prefix + "bcposition"

		self.cursor.execute('''
			UPDATE `{}` SET last_block_id='{}'
			WHERE world='{}'
		'''.format(table, position, world))

		self.connection.commit()



	###
	### save into database
	###
	def save_data(self, world, data):

		table_world = "`{}{}`".format(self.table_prefix, world)

		pick = data["pick"]
		put = data["put"]

		update_query = ('''
			INSERT INTO ''' + table_world + ''' (user, block, pick, put) VALUES (%(user)s, %(block)s, %(pick)s, %(put)s)
			ON DUPLICATE KEY UPDATE pick=pick+%(pick)s, put=put+%(put)s;
		''')

		for user in pick:
			for replaced in pick[user]:
				self.cursor.execute(update_query, {"user": user, "block": replaced, "pick": pick[user][replaced], "put": 0})

		for user in put:
			for type in put[user]:
				self.cursor.execute(update_query, {"user": user, "block": type, "pick": 0, "put": put[user][type]})

		self.connection.commit()
