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

STOPPED:
(1) how to pass info to extract visual features?

"""

import os
import csv
import sys
import errno
import shutil
import zipfile
import boto.ec2
import subprocess


import strategies
from strategies import strategy_factory, extract_frames, extract_visual_features
import s3_utility
import file_utils

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
			s3_utility,
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
		self.s3_utility = s3_utility
		self.input_s3_bucket = input_s3_bucket
		self.output_s3_bucket = output_s3_bucket
		self.processed_s3_bucket = processed_s3_bucket
		self.data_group_id = data_group_id
		self.processing_strategy = processing_strategy
		self.output_dir = output_dir
		self.input_dir = input_dir
		self.region = region
		self.current_filename = ''
		self.upload_count = 0
		self.files_to_process = self.s3_utility.download_file_list(self.input_s3_bucket)

	def file_already_processed(self, filename):
		"""
		:description: checks if a file has already been processed. Does so by checking for existence of the filename in the list of processed files in the input bucket

		:type filename: string
		:param filename: name of the file to process
		"""
		print("file_already_processed")
		# load the processed files from the processed files bucket
		processed_files = self.s3_utility.download_file_list(self.processed_s3_bucket)

		# if the file to process is in the processed files, return true
		return filename in processed_files

	def report_file_finished_processing(self):
		"""
		:description: creates an external note that a file has finished processing. This is to account for if a node stops executing in the middle of processing a file. 
		"""
		print("report_file_finished_processing")
		# write an empty file locally
		path = os.path.join('/home/ec2-user/processed', self.current_filename)
		f = open(path, 'w').close()

		# uplaod the file
		self.s3_utility.upload_file(self.processed_s3_bucket, self.current_filename, path)

	def upload_output(self):
		"""
		:description: upload all files in the output directory to s3
		"""
		print("upload_output")
		# for root, dirs, files in os.walk(self.output_dir):
		# 	for f in files:
		# 		self.upload_file(self.output_s3_bucket, f, os.path.join(root, f))
		zip_name = get_frame_name_from_filename(self.current_filename) + '.zip'
		self.s3_utility.upload_file(self.output_s3_bucket, zip_name, '/home/ec2-user/output.zip')

	def terminate(self):
		"""
		:description: terminates this instance
		"""
		conn = boto.ec2.connect_to_region(self.region, aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)
		conn.terminate_instances(instance_ids=[self.id])

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
		self.s3_utility.download_file(self.input_s3_bucket, self.current_filename, self.input_dir)

		# these if statements are indicative of poor design, need to refactor somehow
		# problem is that these different strategies need different inputs
		if self.processing_strategy == extract_visual_features.extract_visual_features:
			file_utils.unzip_frames_directory(self.current_filename, self.input_dir)

		# call the processing_strategy
		if self.processing_strategy == extract_visual_features.extract_visual_features:
			features = self.processing_strategy(self.input_dir)
			file_utils.write_caffe_features(features, self.output_dir)

		else:
			self.processing_strategy(self.current_filename, self.input_dir, self.output_dir)

		# zip output files
		file_utils.zip_output()

		# upload processed data to the output s3 bucket
		self.upload_output()

		# report a file as having finished processing
		self.report_file_finished_processing()

		# delete current processing output and input file
		file_utils.empty_directory(self.input_dir)
		file_utils.empty_directory(self.output_dir)

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
	strategy_factory = strategy_factory.StrategyFactory()
	strategy_factory.register('extract_frames', extract_frames.extract_frames)
	strategy_factory.register('extract_visual_features', extract_visual_features.extract_visual_features)

	# load variables passed to the instance
	system_variables = file_utils.load_system_variables()

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

	
	if sys.platform == 'linux2':
		# set constant variables ec2
		output_dir = '/home/ec2-user/output/'
		input_dir = '/home/ec2-user/input/'
		processed_dir = '/home/ec2-user/processed/'
	else:
		# set constant variables local
		output_dir = 'processing/output/'
		input_dir = 'processing/input/'
		processed_dir = 'processing/processed/'

	# make the directories
	file_utils.make_directory(output_dir)
	file_utils.make_directory(input_dir)
	file_utils.make_directory(processed_dir)

	# create the s3 utility for the peon to use
	s3_util = s3_utility.S3Utility(access_key, secret_key)

	# load system parameters set by master node into local variables to use in init of peon
	peon = Peon(
		instance_id,
		s3_util,
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

		print("\nFILENAME: {}".format(f))
		peon.process_file(f)

	# once processing completes, terminate this ec2 instance
	#peon.terminate()

