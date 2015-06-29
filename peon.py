"""
improvements to make
(1) add documentation where it's missing
(2) refactor 
	// why not do this today? b/c the RNN should take a while to train, and I can refactor while it is training
	(1) extract out the S3 operations into a separate class / file / module
	(2) extract out file utilities into separate class / file / module
	(3) determine what form peon should have after that
		- seems like it should really just get a file as input and output a file
			- no keeping track of output directories
		- look at the peon constructor params
	(4) unify the input/output of the processing (one file in, one file out?)
		- then extract out aggregation?	// how is this done?
		- this will take some thinking, decide on a unified processing interface
	(5) have the peon script / all the scripts pulled from an s3 bucket before processing?
		- some way to avoid issues with them not being the same?
		- maybe just don't scale up until you are completely confident in the pipeline
	(6) general improvements to the classes 
		- make the connection object only once
		- better error / exception handling
	(7) organization of classes / files
		- should be:
			- peon.py
			- s3_utils.py
			- file_utils.py
			- /strategies
				|- extract_frames.py
				|- extract_visual_features.py
	(8) make the master.py start script a separate file
(3) get components working
	(1) automatic calling of the python script
	(2) passing instance id / retrieve this from python

refactoring questions to answer:
	(1) is the environment variable passing method the best way to pass information? what other ways would be better? Does mapreduce or similar frameworks employ a client/server model or something? How do those frameworks work?
	(2) 

"""

import os
import csv
import sys
import boto
import errno
import shutil
import zipfile
import boto.ec2
import subprocess

# from abc import ABCMeta, abstractmethod

from boto.s3.connection import S3Connection
from boto.s3.key import Key

def make_directory(directory_path):
	# create the output directory if it does not already exist
	try:
		# makes the dir
		os.makedirs(directory_path)
	except OSError as exc: 
		# if the exception is that the directory exists, ignore it so long as it is in fact a directory
		if exc.errno == errno.EEXIST and os.path.isdir(directory_path):
			pass

def get_frame_name_from_filename(filename):
	video_name = os.path.splitext(os.path.basename(filename))[0]
	return video_name

def load_system_variables():
	"""
	:description: load in environment variables passed in by the master node. The checking here is less comprehensive, because it is assumed that certain checks have already been made (e.g., that the buckets exist). Those checks in master node will prevent peon from entering some undesirable state (and if they don't then something has gone wrong there, not here).
	"""
	system_variable_names = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'INPUT_S3_BUCKET', 'OUTPUT_S3_BUCKET', 'PROCESSED_S3_BUCKET', 'DATA_GROUP', 'PROCESSING_OPERATION', 'AWS_REGION']
	system_variable_values = []

	# instance id is checked differently than the other env variables
	# instance_id = os.environ.get('EC2_INSTANCE_ID')
	# assert instance_id is not None, "EC2_INSTANCE_ID does not exist"
	# try:
	# 	instance_id = instance_id.split(' ')[1]
	# except Exception as e:
	# 	print("instance id: {} is not in the expected form of \'instance-id: i-5a4e07ad\'".format(instance_id))
	# 	raise(e)
	# system_variable_values.append(instance_id), let's just sweep this for a bit
	system_variable_values.append("instance-id: i-62f0a495")

	# each of the remaining environment variables is checked in the same way so loop through them
	for name in system_variable_names:
		variable = os.environ.get(name)
		assert variable is not None, "{} does not exist".format(name)
		system_variable_values.append(variable)

	return system_variable_values


class Peon(object):
	"""
	Class that runs on the nodes started by Master that process files

	ways for processing to work:
	(1) load the file list once at the beginning, go through each of the files in there, no changing anything
	(2) (1) except now remove files that have been processed from the list
	(3) for each file to process, check if it has already been processed
		this is the best - keep the file loading and checking separate
	"""

	def __init__(self,
			instance_id,
			access_key,
			secret_key,
			input_s3_bucket,
			output_s3_bucket,
			processed_s3_bucket, 
			data_group_id,
			processing_strategy,
			output_dir,
			input_dir,
			region
			):

		self.id = instance_id
		self.access_key = access_key
		self.secret_key = secret_key
		self.input_s3_bucket = input_s3_bucket
		self.output_s3_bucket = output_s3_bucket
		self.processed_s3_bucket = processed_s3_bucket
		self.data_group_id = data_group_id
		self.processing_strategy = processing_strategy
		self.output_dir = output_dir
		self.input_dir = input_dir
		self.region = region
		self.current_filename = ''
		self.conn = S3Connection(access_key, secret_key)
		self.upload_count = 0
		self.files_to_process = self.load_file_list_from_bucket(self.input_s3_bucket)

	def get_conn(self):
		if self.conn:
			return self.conn
		else:
			return S3Connection(self.access_key, self.secret_key)

	def load_file_list_from_bucket(self, bucket):
		"""
		:description: loads the list of files to process based on the data group id and then checks this list for files that have already been processed, removes those that have been from the list
		"""
		print("load_file_list_from_bucket")
		# create a connection to s3
		# conn = S3Connection(self.access_key, self.secret_key)

		# select the bucket, where input_s3_bucket takes the form 'bsdsdata'
		bucket = self.get_conn().get_bucket(bucket)

		# collect the list of files to process - those that start with the data group id
		file_list = []
		for key in bucket.list():
			if key.name.encode('utf-8').startswith(self.data_group_id):
				file_list.append(key.name.encode('utf-8'))

		return file_list

	def load_file(self, s3_bucket, file_to_load, local_save_dir):
		"""
		:description: load a file from a given s3 bucket with a given name to a given local dir

		:type s3_bucket: string
		:param s3_bucket: s3 bucket from which to load the file

		:type file_to_load: string
		:param file_to_load: the file to load

		:type local_save_dir: string
		:param local_save_dir: the local dir to which to save the downloaded file
		"""
		print("load_file")
		# create a connection to s3
		#conn = S3Connection(self.access_key, self.secret_key)

		# select the bucket, where input_s3_bucket takes the form 'bsdsdata'
		bucket = self.get_conn().get_bucket(s3_bucket)

		# set a key to the processed files list
		key = Key(bucket, file_to_load)

		# download the file to process and save in the input location
		save_location = os.path.join(local_save_dir, key.name.encode('utf-8'))
		try:
			key.get_contents_to_filename(save_location)
		except boto.exception.S3ResponseError as e:
			print("key name: {} failed".format(key.name.encode('utf-8')))
			raise(e)

		# return the location of the downloaded file
		return save_location

	def file_already_processed(self, filename):
		"""
		:description: checks if a file has already been processed. Does so by checking for existence of the filename in the list of processed files in the input bucket

		:type filename: string
		:param filename: name of the file to process
		"""
		print("file_already_processed")
		# load the processed files from the processed files bucket
		processed_files = self.load_file_list_from_bucket(self.processed_s3_bucket)

		# if the file to process is in the processed files, return true
		return filename in processed_files

	def upload_file(self, s3_bucket, filename_to_save_as, file_path):
		"""
		:description: uploads a single file to an s3 bucket

		:type s3_bucket: string
		:param s3_bucket: name of the s3 bucket to which the file should be uploaded
		"""
		self.upload_count += 1
		print(self.upload_count)
		# what is this?
		def percent_cb(complete, total):
			sys.stdout.write('.')
			sys.stdout.flush()

		# create a connection to s3
		#conn = S3Connection(self.access_key, self.secret_key)

		# select the bucket, where input_s3_bucket takes the form 'bsdsdata'
		bucket = self.get_conn().get_bucket(s3_bucket)

		# send the file to the s3 bucket
		key = Key(bucket)
		key.key = filename_to_save_as
		key.set_contents_from_filename(file_path, cb=percent_cb, num_cb=50)

	def report_file_finished_processing(self):
		"""
		:description: creates an external note that a file has finished processing. This is to account for if a node stops executing in the middle of processing a file. 
		"""
		print("report_file_finished_processing")
		# write an empty file locally
		path = os.path.join('/home/ec2-user/processed', self.current_filename)
		f = open(path, 'w').close()

		# uplaod the file
		self.upload_file(self.processed_s3_bucket, self.current_filename, path)

	def zip_output(self):
		zipf = zipfile.ZipFile('/home/ec2-user/output.zip', 'w')
		for root, dirs, files in os.walk(self.output_dir):
			for file in files:
				zipname = get_frame_name_from_filename(file) + '.zip'
				zipname = os.path.join('/home/ec2-user/output', zipname)
				zipf.write(os.path.join('/home/ec2-user/output', file))

	def upload_output(self):
		"""
		:description: upload all files in the output directory to s3
		"""
		print("upload_output")
		# for root, dirs, files in os.walk(self.output_dir):
		# 	for f in files:
		# 		self.upload_file(self.output_s3_bucket, f, os.path.join(root, f))
		zip_name = get_frame_name_from_filename(self.current_filename) + '.zip'
		self.upload_file(self.output_s3_bucket, zip_name, '/home/ec2-user/output.zip')

	def delete_previous_output_and_input_file(self):
		"""
		:description: delete the input and output files/directories and then remake them
		"""
		print("delete_previous_output_and_input_file")
		# delete the input and output directories
		shutil.rmtree(self.input_dir)
		shutil.rmtree(self.output_dir)

		# remake them
		make_directory(self.input_dir)
		make_directory(self.output_dir)

	def terminate(self):
		"""
		:description: terminates this instance
		"""
		conn = boto.ec2.connect_to_region(self.region, aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)
		conn.terminate_instances(instance_ids=[self.id])

	# def extract_frames(self):
	# 	filename_base = get_frame_name_from_filename(self.current_filename)
	# 	output_filepath_base = os.path.join(self.output_dir, filename_base)
	# 	input_filename = os.path.join(self.input_dir, self.current_filename)
	# 	frames_per_second = 30
	# 	call_str = 'ffmpeg -i {0} -r {1} {2}_%03d.jpg'.format(input_filename, frames_per_second, output_filepath_base)
	# 	FNULL = open(os.devnull, 'w')
	# 	subprocess.call(call_str, shell=True, stderr=subprocess.STDOUT)

	def process_file(self, filename):
		"""
		:description: selects from a group of file processing options and executes the choosen option
			(1) sets the current filename to the one passed in
			(2) loads the file from the s3 bucket
		"""
		print("process_file")

		# check that this file has not already been processed, if so, skip it
		if self.file_already_processed(filename):
			return

		# set the current file to process
		self.current_filename = filename

		# load file into this ec2 instance
		self.load_file(self.input_s3_bucket, self.current_filename, self.input_dir)

		# call the processing_strategy
		self.processing_strategy(self.current_filename, self.input_dir, self.output_dir)

		# zip output files
		self.zip_output()

		# upload processed data to the output s3 bucket
		self.upload_output()

		# report a file as having finished processing
		self.report_file_finished_processing()

		# delete current processing output and input file
		self.delete_previous_output_and_input_file()


def extract_frames(filename, input_dir, output_dir):
	# determine the base name of this filename for use in building the output filename
	filename_base = get_frame_name_from_filename(filename)

	# set the base path of the output files
	output_filepath_base = os.path.join(output_dir, filename_base)

	# set the file path of the input file
	input_filepath = os.path.join(input_dir, filename)

	# set the frames per second
	frames_per_second = 30

	# set the call string
	call_str = 'ffmpeg -i {0} -r {1} {2}_%03d.jpg'.format(input_filepath, frames_per_second, output_filepath_base)
	FNULL = open(os.devnull, 'w')

	# call the subprocess
	subprocess.call(call_str, shell=True, stderr=subprocess.STDOUT)

def extract_visual_features(filename, input_dir, output_dir):
	raise NotImplementedError("feature extraction not yet implemented")

class StrategyFactory(object):
	"""
	:description: in python, a factory can be a dictionary that maps identifiers to either classes or functions. I'm making this a class in order to keep the error handling out of the main function.
	"""

	def __init__(self):
		self.strategies = dict()

	def register(self, id, strategy):
		"""
		:description: registers a strategy with the factory - overwrites previous strategies with the same id and ids can be anything

		:type id: anything?
		:param id: the id used to retrieve a given strategy

		:type strategy: a function
		:param strategy: the function to call as the strategy
		"""
		self.strategies[id] = strategy

	def get_strategy(self, id):
		try:
			return self.strategies[id]
		except KeyError as e:
			raise KeyError("the provided strategy id does not exist or is not registered")

if __name__ == '__main__':
	"""
	:description:
		(0) checks that approriate files/directories are set up
		(1) collections environment vairables set by the master node in local variables
		(2) instantiate a peon and in doing so load the file list of files to process
		(3) while the list of files to process is not empty 
			(a) if file has already been processed, remove from list
			(b) else process the file
	"""
	# log that this script ran
	f = open("script_ran.txt", 'w').close()

	# initialize the "strategy factory", which is just a dict() in python (in this case wrapped in a class to handle key validation)
	strategy_factory = StrategyFactory()
	strategy_factory.register('extract_frames', extract_frames)
	strategy_factory.register('extract_visual_features', extract_visual_features)

	# load variables passed to the instance
	system_variables = load_system_variables()
	print(system_variables)
	instance_id = system_variables[0]
	access_key = system_variables[1]
	secret_key = system_variables[2]
	input_s3_bucket = system_variables[3]
	output_s3_bucket = system_variables[4]
	processed_s3_bucket = system_variables[5]
	data_group_id = system_variables[6]
	# print("get strategy: {}".format(strategy_factory.get_strategy(system_variables[7])))
	processing_strategy = strategy_factory.get_strategy(system_variables[7])
	region = system_variables[8]

	# set constant variables
	output_dir = '/home/ec2-user/output/'
	input_dir = '/home/ec2-user/input/'
	processed_dir = '/home/ec2-user/processed/'

	# make the directories
	make_directory(output_dir)
	make_directory(input_dir)
	make_directory(processed_dir)

	# load system parameters set by master node into local variables to use in init of peon
	peon = Peon(
		instance_id,
		access_key,
		secret_key,
		input_s3_bucket,
		output_s3_bucket,
		processed_s3_bucket,
		data_group_id,
		processing_strategy,
		output_dir,
		input_dir,
		region
		)

	# while list of files to process is not empty execute the following loop
	for f in peon.files_to_process:
		# print f
		print("\nFILENAME: {}".format(f))

		# process the next file
		peon.process_file(f)

	# once processing completes, terminate this ec2 instance
	#peon.terminate()

