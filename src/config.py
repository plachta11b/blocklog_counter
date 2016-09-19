import yaml

from os import path

class Config:

	path_to_config = "" # class variable shared by all instances
	file_config = None

	def __init__(self, path_to_config):

		script_directory = path.dirname(path.realpath(__file__))

		self.path_to_config = path_to_config

		try:
			config_file_descriptor = open(path.join(script_directory, path_to_config), 'r')
		except IOError as error:
			if error.args[0] == 2:
				print("Did you have created config from config example?")
				
			exit("ERROR Can not open config file: " + error.args[1])

		self.file_config = yaml.load(config_file_descriptor)

	def get_source_connection_configuration(self):
		return self.file_config["connection_source"]

	def get_destination_connection_configuration(self):
		return self.file_config["connection_destination"]

	def get_world_names(self):
		return self.file_config["worlds_to_proccess"]

	def get_maximum_processed(self):
		return self.file_config["maximum_processed"]

	def is_debug_mode_on(self):
		return self.file_config["debugging"]

	def is_agresive_process_on(self):
		return self.file_config["agresive_process"]

	def get_table_prefix(self):
		return self.file_config["table_prefix"]