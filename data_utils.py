import os
import csv
import glob
import argparse

import numpy as np

import file_utils



def get_interaction_name(filepath):
	"""
	:description: returns a the name of the interaction a given data file is part of. Interaction name just puts the male id first

	>>> get_interaction_name('/test/0209_m01_f01.csv')
	'0209_m01_f01'

	>>> get_interaction_name('/test/0209_f01_m01.csv')
	'0209_m01_f01'
	"""
	filename = os.path.splitext(os.path.basename(filepath))[0]
	date = filename[0:4]
	individual = filename[5:8]
	interlocutor = filename[9:12]
	if individual.startswith('m'):
		interaction_name = '{0}_{1}_{2}'.format(date, individual, interlocutor)
	elif individual.startswith('f'):
		interaction_name = '{0}_{2}_{1}'.format(date, individual, interlocutor)
	else: 
		interaction_name = ''
	return interaction_name

def get_filepath_pairs_by_interaction(filepaths):
	"""
	returns a list of tuples of filepaths [(f1,f2), (f3,f4), ...]
	"""
	interaction_name_dict = dict()
	for f in filepaths:
		interaction_name = get_interaction_name(f)
		if interaction_name == '':
			continue
		if interaction_name not in interaction_name_dict:
			interaction_name_dict[interaction_name] = (f, )
		else:
			if interaction_name_dict[interaction_name][0] != f:
				interaction_name_dict[interaction_name] = interaction_name_dict[interaction_name] + (f, )
	return [value for key, value in interaction_name_dict.iteritems() if len(value) == 2]

def group_single_timestep_data_into_sequences(timestep_data, sequence_length):
	"""
	:description: converts a list of lists to a list of lists, where the each sublist contains sequence_length original sublists. Cannot just use numpy.split b/c does not account for subsequences. Some combination of numpy transforms may work though and if this is slow then that's the first thing to try.

	>>> sequence = [[1,2], [3,4], [5,6], [7,8]]
	>>> group_single_timestep_data_into_sequences(sequence, 2)
	[[1,2,3,4], [5,6,7,8]]

	"""
	data = np.array(timestep_data)

	# discard rows to allow for even reshape
	try:
		n_cur_rows = data.shape[0]
		n_cur_cols = data.shape[1]
	except IndexError as e:
		raise IndexError('in group_single_timestep_data_into_sequences(), the timestep_data did not have a valid shape value')
	n_rows_to_discard = n_cur_rows % sequence_length
	if n_rows_to_discard != 0:
		data = data[:-n_rows_to_discard]
		n_cur_rows -= n_rows_to_discard


	n_new_cols = n_cur_cols * sequence_length
	n_new_rows = n_cur_rows / sequence_length
	sequences = np.reshape(data, (n_new_rows, n_new_cols))
	return sequences.tolist()

def column_stack_with_concatenate(data_1, data_2):
	"""
	:description: column stacks two arrays. Not using np.column_stack b/c may be different lengths

	>>> data_1 = [[1,2],[3,4]]
	>>> data_2 = [['a','b'],['c','d']]
	>>> column_stack_with_concatenate(data_1, data_2)
	[[1,2,'a','b'],[3,4,'c','d']]

	"""
	return [i + j for i,j in zip(data_1, data_2)]

def create_sample_from_features(data_1, data_2, sequence_length):
	"""
	:description: chains the calls to group_single_timestep_data_into_sequences() and column_stack_with_concatenate() for a given pair of data arrays. Mostly for easier testing.
	"""
	data_1 = group_single_timestep_data_into_sequences(data_1, sequence_length)
	data_2 = group_single_timestep_data_into_sequences(data_2, sequence_length)
	sample = column_stack_with_concatenate(data_1, data_2)
	return sample

def create_samples_from_feature_files(pairs, sequence_length, output_filepath):
	"""
	:description: 
	"""
	samples = []
	for pair in pairs:
		data_1 = file_utils.load_caffe_features(pair[0])
		print('data_1.shape: {}'.format(data_1.shape))
		print('type(data_1): {}'.format(type(data_1)))
		data_2 = file_utils.load_caffe_features(pair[1])
		if not data_1.size or not data_2.size:
			continue
		sample = create_sample_from_features(data_1, data_2, sequence_length)
		print('sample.shape: {}'.format(np.shape(sample)))
		print('type(sample): {}'.format(type(sample)))
		#file_utils.append_sample(output_filepath, sample)
		#samples += sample
	return samples

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--feature_directory',dest='feat_dir', type=str, help='dir containing feat files')
	parser.add_argument('--output_filepath',dest='output_filepath', type=str, help='output filepath')
	parser.add_argument('--sequence_length',dest='sequence_length', type=int, help='sequence_length')
	args = parser.parse_args()

	feature_directory = args.feat_dir
	output_filepath = args.output_filepath
	sequence_length = args.sequence_length

	if not os.path.exists(feature_directory):
		raise RuntimeError("Output directory does not exist %s"%(feature_directory))

	filepaths = file_utils.load_filenames_from_directory(feature_directory)
	pairs = get_filepath_pairs_by_interaction(filepaths)
	# samples = create_samples_from_feature_files(pairs, sequence_length)
	samples = create_samples_from_feature_files(pairs, sequence_length, output_filepath)

	np.save(output_filepath, samples)
	#np.savetxt(output_filepath, samples, delimiter=',')
	#file_utils.write_data_to_file(samples, output_filepath)
