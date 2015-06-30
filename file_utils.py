import os
import sys
import csv
import glob
import errno
import shutil
import zipfile

import numpy as np

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

def empty_directory(dir):
	"""
	:description:
	"""
	shutil.rmtree(dir)
	make_directory(dir)

# def delete_previous_output_and_input_file(self):
# 	# maybe better as reset directory
# 		"""
# 		:description: delete the input and output files/directories and then remake them
# 		"""
# 		print("delete_previous_output_and_input_file")
# 		# delete the input and output directories
# 		shutil.rmtree(self.input_dir)
# 		shutil.rmtree(self.output_dir)

# 		# remake them
# 		make_directory(self.input_dir)
# 		make_directory(self.output_dir)

def create_empty_file(filepath):
	"""
	:description: 
	"""
	open(path, 'w').close()

def get_frame_name_from_filename(filename):
	"""
	:description:
	"""
	video_name = os.path.splitext(os.path.basename(filename))[0]
	return video_name

def load_filenames_from_directory(directory, filter_string='*'):
	search_string = '{0}/{1}'.format(directory, filter_string)
	return glob.glob(search_string)

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

def unzip_file(self):
	raise NotImplementedError

def read_data(input_filename):
	"""
	:description: reads in a csv file returning a list of lists, where each sublist is a row in the file
	"""
	data = []
	try:
		with open(input_filename, 'r') as csvfile:
			reader = csv.reader(csvfile, delimiter=',')
			for row in reader:
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
	