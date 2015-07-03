import os
import sys
import csv
import glob
import errno
import shutil
import zipfile
import scipy.io

import numpy as np


################
### filename ###
################
def get_frame_name_from_filename(filename):
	"""
	:description:
	"""
	video_name = os.path.splitext(os.path.basename(filename))[0]
	return video_name

def load_filenames_from_directory(directory, filter_string='*'):
	search_string = '{0}/{1}'.format(directory, filter_string)
	return glob.glob(search_string)

################
### file sys ###
################
def load_system_variables():
	"""
	:description: load in environment variables passed in by the master node. The checking here is less comprehensive, because it is assumed that certain checks have already been made (e.g., that the buckets exist). Those checks in master node will prevent peon from entering some undesirable state (and if they don't then something has gone wrong there, not here).
	"""
	system_variable_names = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'INPUT_S3_BUCKET', 'OUTPUT_S3_BUCKET', 'PROCESSED_S3_BUCKET', 'DATA_GROUP', 'PROCESSING_OPERATION', 'AWS_REGION']
	system_variable_values = []

	system_variable_values.append("instance-id: i-62f0a495")

	# each of the remaining environment variables is checked in the same way so loop through them
	for name in system_variable_names:
		variable = os.environ.get(name)
		assert variable is not None, "{} does not exist".format(name)
		system_variable_values.append(variable)

	return system_variable_values

################
### file ops ###
################
def zip_output(output_dir):
	"""
	:description:
	"""
	zipf = zipfile.ZipFile('/home/ec2-user/output.zip', 'w')
	for root, dirs, files in os.walk(output_dir):
		for file in files:
			zipname = get_frame_name_from_filename(file) + '.zip'
			zipname = os.path.join('/home/ec2-user/output', zipname)
			zipf.write(os.path.join('/home/ec2-user/output', file))

def unzip_file(filepath):
	raise NotImplementedError

def correct_frames_directory(directory):
	"""
	:description: takes the files in ../directory/home/ec2-user/output and moves them to ../directory/
	"""
	path_to_dir_to_move = os.path.join(directory, 'home/ec2-user/output/')
	files = glob.glob('{0}/*.jpg'.format(path_to_dir_to_move))
	for f in files:
		shutil.move(f, directory)
	dir_to_remove = os.path.join(directory, 'home')
	shutil.rmtree(dir_to_remove)

def unzip_frames_directory(filename, directory):
	"""
	:description: extracts all of the files in the filepath archive, then fixes the file structure.
	"""
	filepath = os.path.join(directory, filename)
	with zipfile.ZipFile(filepath, 'r') as zf:
		zf.extractall(directory)
	correct_frames_directory(directory)

def make_directory(directory_path):
	"""
	:description:
	"""
	# create the output directory if it does not already exist
	try:
		# makes the dir
		os.makedirs(directory_path)
	except OSError as exc: 
		# if the exception is that the directory exists, ignore it so long as it is in fact a directory
		if exc.errno == errno.EEXIST and os.path.isdir(directory_path):
			pass
		else:
			print('unable to make directory: {}'.format(directory_path))
			raise exc

def empty_directory(dir):
	"""
	:description:
	"""
	shutil.rmtree(dir)
	make_directory(dir)

################
### file i/o ###
################
def read_data(input_filename):
	"""
	:description: reads in a csv file returning a list of lists, where each sublist is a row in the file
	"""
	data = []
	try:
		with open(input_filename, 'r') as csvfile:
			reader = csv.reader(csvfile, delimiter=' ')
			for index, row in enumerate(reader):
				if index % 30 == 0:
					data.append(map(float,row))
	except IOError as e:
		raise IOError('input filename: {} raised IOError on read'.format(input_filename))
	return data

def write_data_to_file(data, output_filename):
	"""
	Writes data to an output file. 
	data in the format [frame_number, smile=1 or nosmile=0]
	"""
	with open(output_filename, 'w') as csvfile:
		writer = csv.writer(csvfile, delimiter=',')
		for row in data:
			writer.writerow(row)

def append_sample(output_filepath, sample):
	print("append_sample")
	print(sample)
	with open(output_filepath, 'ab') as f:
		np.save(f, sample)

def load_filenames_from_directory(directory, filter_string='*'):
	search_string = '{0}/{1}'.format(directory, filter_string)
	return glob.glob(search_string)

def create_empty_file(filepath):
	"""
	:description: 
	"""
	open(path, 'w').close()

def load_caffe_features(filepath):
	load_dict = scipy.io.loadmat(filepath)
	return load_dict['features'].T

def save_caffe_features(features, output_dir, filename):
	save_dict = {}
	save_dict['features'] = features
	scipy.io.savemat(filepath, save_dict)
	


	