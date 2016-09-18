class SourceDatabase:

	connection = None
	cursor = None

	def __init__(self, connection):

		self.connection = connection

		# make cursor for fetch data
		self.cursor = connection.cursor()

	def get_sql_select(self, world):
		return ('''
			SELECT id, playerid, replaced, type, data
			FROM `''' + world + '''`
			WHERE id BETWEEN %s AND %s
		''')

	def fetch_data_from(self):
		raise NotImplementedError

	def fetch_data_from_to(self, world, select_start, select_end):

		self.cursor.execute(self.get_sql_select(world), [select_start, select_end - 1])

		description = self.cursor.description

		sql_data = self.cursor.fetchall()

		# The number of rows is -1 immediately after query execution and is incremented as rows are fetched.
		sql_row_count = self.cursor.rowcount

		return SourceData(sql_data, description, sql_row_count)


class SourceData:

	sql_data = None
	columns_description = None
	count = -1

	def __init__(self, sql_data, description, row_count):
		self.sql_data = sql_data
		self.columns_description = description
		self.count = row_count

	def get_row_count(self):
		return self.count

	def get_item(self):
		for value in self.sql_data:
			tmp = {}
			for (index,column) in enumerate(value):
				 tmp[self.columns_description[index][0]] = column
			yield tmp

class NoDataToProceed(Exception):
	"""No data to be processed"""