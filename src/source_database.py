class SourceDatabase:

	connection = None
	cursor = None

	table_data = None
	table_data_columns = None
	table_data_row_count = 0

	select_data_from_to_structure = None

	def __init__(self, connection, database_name, world):

		# self.connection = get_connection(database_name)
		self.connection = connection

		# make cursor for fetch data
		self.cursor = connection.cursor()

		self.select_data_from_to_structure = ('''
			SELECT id, playerid, replaced, type, data
			FROM ''' + world + '''
			WHERE id BETWEEN %s AND %s
		''')

	def fetch_data_from(self):
		raise NotImplementedError

	def fetch_data_from_to(self, select_start, select_end):

		self.cursor.execute(self.select_data_from_to_structure, [select_start, select_end - 1])

		if self.cursor.rowcount == 0:
			data = None

		self.table_data_columns = self.cursor.description

		self.database_data = self.cursor.fetchall()

		# The number of rows is -1 immediately after query execution and is incremented as rows are fetched.
		self.table_data_row_count = self.cursor.rowcount

		self.cursor.close()
		self.connection.close()

	def get_row_count(self):

		if self.table_data is None:
			NoDataToProceed("ERROR: There is no data. You should fetch some first")

		return self.table_data_row_count

	def get_item(self):

		if self.table_data is None:
			NoDataToProceed("ERROR: There is no data. You should fetch some first")

		for value in self.database_data:
			tmp = {}
			for (index,column) in enumerate(value):
				 tmp[self.table_data_columns[index][0]] = column
			yield tmp

class NoDataToProceed(Exception):
	"""No data to be processed"""