"""
filename: master.py
author: blake wulfe
description: contains Master class for creating processing nodes and driver for operating master

improvements
	(1) make conn a member of master (when called if does not exist creates one)

example call:

	sudo python master.py --id i-5a4e07ad --access_key <access_key> --secret_key <secret_key> --input_s3_bucket bsdsdata --output_s3_bucket bsdsframes --region us-west-2 --ami ami-6df8fd5d --subnet subnet-7c952819 --processed_s3_bucket bsdsprocessedfiles

example bashrc:



"""

import os
import time
import boto.ec2
import argparse

from boto.s3.connection import S3Connection

class Master(object):
	"""
	Class that starts peon nodes and controls execution
	"""

	def __init__(self, 
				instance_id,
				access_key,
				secret_key,
				input_s3_bucket,
				output_s3_bucket,
				processed_s3_bucket,
				region,
				ami,
				subnet,
				instance_type,
				processing_operation,
				max_n_peons):

		self.master_id = instance_id
		self.access_key = access_key
		self.secret_key = secret_key
		self.input_s3_bucket = input_s3_bucket
		self.output_s3_bucket = output_s3_bucket
		self.processed_s3_bucket = processed_s3_bucket
		self.region = region
		self.ami = ami
		self.subnet = subnet
		self.instance_type = instance_type
		self.processing_operation = processing_operation
		self.max_n_peons = max_n_peons
		self.peon_ids = []
		self.start_time = time.time()
		self.load_data_group_names()

	def get_group_names_from_filenames(self, filenames):
		"""
		:description: return a list of group_names from the list of filenames

		:type filenames: list of strings
		:parma filenames: the filenames to separate into groups
		"""
		# init a dict to hold the names
		group_names = dict()

		# go through the filenames adding the group name to the dict
		for f in filenames:

			# get the groupname == first four chars in this case
			group_name = f[:4]

			# add that group name to the dict
			group_names[group_name] = True

		# returns a list of the keys
		return list(group_names)

	def load_data_group_names(self):
		"""
		:description: loads the data groups (bins of files to be processed)
		"""
		# create a connection to s3
		conn = S3Connection(self.access_key, self.secret_key)

		# select the bucket, where input_s3_bucket takes the form 'bsdsdata'
		bucket = conn.get_bucket(self.input_s3_bucket)

		# create a list of the filenames
		filenames = [key.name.encode('utf-8') for key in bucket.list()]

		# set self.data_group_names with the individual group name
		self.data_group_names = self.get_group_names_from_filenames(filenames)

	def terminate(self):
		"""
		:description: terminates this instance
		"""
		conn = boto.ec2.connect_to_region(self.region, aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)
		conn.terminate_instances(instance_ids=[self.master_id])

	def get_running_peons(self):
		"""
		:description: returns a list of the ids of 
		"""
		# connect to the region
		conn = boto.ec2.connect_to_region(self.region, aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)

		# get all the reservations in the region
		reservations = conn.get_all_reservations()

		# each reservation corresponds to a number of instances, get those
		instances = [res.instances for res in reservations]

		# collect a list of running instances
		running_instances = []
		for instance_list in instances:
			for inst in instance_list:
				if inst.state == "running":
					running_instances.append(inst)

		# return the list
		return running_instances

	def report(self):
		"""
		:description: reports on the progress of the file processing
		"""
		print('seconds elapsed: {}'.format(time.time() - self.start_time))
		running_peons = self.get_running_peons()
		print('{0} peons still running\n'.format(len(running_peons)))

	def processing_complete(self):
		"""
		:description: collect a list of running instances b/c if they are running then they are still processing
		could instead check that the s3 list of completed videos contains all the videos to be processed
		that would probably be better actually
		"""
		# create a connection to s3
		conn = S3Connection(self.access_key, self.secret_key)

		# select the bucket, where input_s3_bucket takes the form 'bsdsdata'
		bucket = conn.get_bucket(self.input_s3_bucket)

		# set the length of a valid file name
		filename_len = 16

		# collect the list of files to process - those that start with the data group id
		files_to_process = []
		for key in bucket.list():
			if len(key.name) == filename_len:
				files_to_process.append(key.name)

		# set the bucket to the processed files
		bucket = conn.get_bucket(self.processed_s3_bucket)

		# collect the list of files to process - those that start with the data group id
		processed_files = []
		for key in bucket.list():
			if len(key.name) == filename_len:
				processed_files.append(key.name)

		# if they contain the same files then processing is complete
		return set(files_to_process) == set(processed_files)

	def start_peon(self, data_group):
		"""
		:description: starts a single ec2 instance by making a call to create_instance above. Info for the self values given:

		:type ami: string
		:param ami: the ami id from which to create the instance

		:type instance_type: string
		:param instance_type: the type of instance to use (e.g., t2.micro)

		:type region: string
		:param region: the region to create the instance in (e.g., us-west-2)

		:type subnet: string
		:param subnet: the subnet to use. Retrieved from the vpc screens (e.g., subnet-7c952819)

		:type start_script: string
		:param start_script: string passed to the new instance to run. Should ultimately be a script instead.

		"""
		# there is a better way that I should use
		# add "python ~/scripts/peon.py" (or something like it to run the peon.py script)
		start_script = '''#!/bin/bash
		echo \"data_group\"
		echo export DATA_GROUP={0} >> /home/ec2-user/.bashrc
		echo \"access key\"
		echo export AWS_ACCESS_KEY_ID={1} >> /home/ec2-user/.bashrc
		echo echo \"secret key\"
		echo export AWS_SECRET_ACCESS_KEY={2} >> /home/ec2-user/.bashrc
		echo \"input s3 bucket\"
		echo export INPUT_S3_BUCKET={3} >> /home/ec2-user/.bashrc
		echo \"output s3 bucket\"
		echo export OUTPUT_S3_BUCKET={4} >> /home/ec2-user/.bashrc
		echo \"processing s3 bucket\"
		echo export PROCESSED_S3_BUCKET={5} >> /home/ec2-user/.bashrc
		echo \"processing op\"
		echo export PROCESSING_OPERATION={6} >> /home/ec2-user/.bashrc
		echo \"processing op\"
		echo export AWS_REGION={7} >> /home/ec2-user/.bashrc
		echo \"instance id\"
		echo export EC2_INSTANCE_ID=$(ec2-metadata --instance-id) >> /home/ec2-user/.bashrc
		source /home/ec2-user/.bashrc
		python /home/ec2-user/peon.py

		'''.format(data_group, self.access_key, self.secret_key, self.input_s3_bucket, self.output_s3_bucket, self.processed_s3_bucket, self.processing_operation, self.region)
			
		# connect to the region
		conn = boto.ec2.connect_to_region(self.region, aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)

		# start the instance
		conn.run_instances(self.ami, subnet_id=self.subnet, instance_type=self.instance_type, user_data = start_script)

	def start_peons(self):
		"""
		:description: starts an ec2 instance for each of the data groups
		"""
		for data_group, index in zip(self.data_group_names, range(self.max_n_peons)):
			self.start_peon(data_group)


if __name__ == '__main__':

	"""
	:description:
		(1) instantiates a Master class, master, with arguments from command line
		(2) master.load_data_group_names()	// loads the names of the different data groups that each node will process
		(3) master.start_peons()	// starts the different peon instances
		(4) while not master.processing_complete()	// check if processing finsihed
			(a) report on what fraction of videos processed every time unit
		(5) when that while loop finishes, self.terminate()		// kill this ec2 instance
	"""
	parser = argparse.ArgumentParser()
	parser.add_argument('--id', type=str, dest='instance_id', help='the id of this ec2 instance')
	parser.add_argument('--access_key', type=str, dest='access_key', help='aws access key')
	parser.add_argument('--secret_key', type=str, dest='secret_key', help='aws secret key')
	parser.add_argument('--input_s3_bucket', type=str, dest='input_s3_bucket', help='input s3 bucket path')
	parser.add_argument('--output_s3_bucket', type=str, dest='output_s3_bucket', help='output s3 bucket path')
	parser.add_argument('--processed_s3_bucket', type=str, dest='processed_s3_bucket', help='processed s3 bucket path')
	parser.add_argument('--region', type=str, dest='region', default='us-west-2', help='aws region')
	parser.add_argument('--ami', type=str, dest='ami', help='the instance ami for the peons')
	parser.add_argument('--subnet', type=str, dest='subnet', help='the vpc subnet to use for the peons')
	parser.add_argument('--instance_type', type=str, dest='instance_type', default='t2.micro', help='the vpc subnet to use for the peons')
	parser.add_argument('--op', type=str, dest='op', default='extract_frames', help='the processing operation')
	parser.add_argument('--max_n_peons', type=int, dest='max_n_peons', default=1, help='max num peons')
	args = parser.parse_args()

	#########################
	### argument checking ###
	#########################

	# check master instance id
	# id_len = 10
	# id_prefix = 'i-'
	# assert len(args.instance_id) == id_len, "ec2 instance id: {} invalid, incorrect length".format(args.instance_id)
	# assert args.instance_id[:len(id_prefix)] == id_prefix, "ec2 instance id: {0} invalid, prefix is not {1}".format(args.instance_id, id_prefix)

	# check that access key id and secret key are valid and that input s3 bucket is available
	conn = S3Connection(args.access_key, args.secret_key)
	try:
		bucket = conn.get_bucket(args.input_s3_bucket)
	except Exception as e:
		print("Bucket: {} does not exist.\n".format(args.input_s3_bucket))
		raise(e)

	# check output s3 bucket is available - should be created prior to processing data
	try:
		bucket = conn.get_bucket(args.output_s3_bucket)
	except Exception as e:
		print("Bucket: {} does not exist.\n".format(args.output_s3_bucket))
		raise(e)

	# check processed s3 bucket is available - should be created prior to processing data
	try:
		bucket = conn.get_bucket(args.processed_s3_bucket)
	except Exception as e:
		print("Bucket: {} does not exist.\n".format(args.processed_s3_bucket))
		raise(e)

	# check region in form ex us-west-2
	assert len(args.region.split('-')) == 3, "aws region: {} in invalid form".format(args.region)

	# check ami image value
	ami_len = 12
	ami_prefix = 'ami-'
	assert len(args.ami) == ami_len, "ami id: {} invalid, incorrect length".format(args.ami)
	assert args.ami[:len(ami_prefix)] == ami_prefix, "aws id: {0} invalid, prefix is not {1}".format(args.ami, ami_prefix)

	# check subnet
	subnet_len = 15
	subnet_prefix = 'subnet-'
	assert len(args.subnet) == subnet_len, "subnet: {} invalid, incorrect length".format(args.subnet)
	assert args.subnet[:len(subnet_prefix)] == subnet_prefix, "subnet: {0} invalid, prefix is not {1}".format(args.subnet, subnet_prefix)

	# check instance type
	assert len(args.instance_type.split('.')) == 2, "ec2 instance type: {} in invalid form".format(args.instance_type)

	###############################
	### running the master node ###
	###############################

	# instantiate a master node
	# the data groups are loaded during initialization
	master = Master(
				args.instance_id,
				args.access_key,
				args.secret_key,
				args.input_s3_bucket,
				args.output_s3_bucket,
				args.processed_s3_bucket,
				args.region,
				args.ami,
				args.subnet,
				args.instance_type,
				args.op,
				args.max_n_peons
			)

	# confirm that files have not already completed processing
	assert not master.processing_complete(), "files have already been processed"

	# start peons
	print("\nSTARTING PEONS, STAND BACK\n")
	master.start_peons()

	# set time to wait between reports
	time_unit = 10	# seconds
	total = 0

	# while processing is not complete, report on progress every time_unit of time
	while not master.processing_complete():
		master.report()
		time.sleep(time_unit)
		total += time_unit
		if total > 5:
			break


	# once processing completes, print a final report 
	master.report()
	
	# terminate this instance - TURN ON ONCE WE GET GOING
	# master.terminate()


