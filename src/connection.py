import mysql.connector
from mysql.connector import errorcode

class Connection:

	config = None

	def __init__(self, config):
		self.config = config

	###
	### Get connection by database name
	###
	def get_connection(self, database):

		connection_configuration = self.config.get_connection_configuration();

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