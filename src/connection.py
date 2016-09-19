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

		if database == "source":
			connection_configuration = self.config.get_source_connection_configuration();
		elif database == "destination":
			connection_configuration = self.config.get_destination_connection_configuration();
		else:
			exit("ERROR: Choose source or destination database")

		# get database connection
		try:
			connection = mysql.connector.connect(**connection_configuration)
		except mysql.connector.Error as error:
			if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				exit("ERROR: Something is wrong with your user name or password")
			elif error.errno == errorcode.ER_BAD_DB_ERROR:
				exit("ERROR: Database '" + database + "' does not exists")
			else:
				exit(error)
		else:
			return connection;