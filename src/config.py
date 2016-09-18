import yaml

from os import path

class Config:

	path_to_config = "" # class variable shared by all instances
	file_config = None

	def __init__(self, path_to_config):

		script_directory = path.dirname(path.realpath(__file__))

		self.path_to_config = path_to_config
		self.file_config = yaml.load(open(path.join(script_directory, "./../config/config.yml"), 'r'))

	def get_connection_configuration(self):
		return self.file_config["connection"]

	def get_source_database(self):
		return self.file_config["database_source"]

	def get_destination_database(self):
		return self.file_config["database_destination"]

	def get_world_names(self):
		return self.file_config["worlds_to_proccess"]

	def get_maximum_processed(self):
		return self.file_config["maximum_processed"]

	def is_debug_mode_on(self):
		return self.file_config["debugging"]
	def get_table_prefix(self):
		return "bc_"
		# return self.file_config["table_prefix"]