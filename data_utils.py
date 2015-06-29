import os
import csv
import glob

import numpy as np

from file_utils import load_filenames_from_directory, read_data

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
	n_cur_rows = data.shape[0]
	n_rows_to_discard = n_cur_rows % sequence_length
	if n_rows_to_discard != 0:
		data = data[:-n_rows_to_discard]

	n_cur_cols = data.shape[1]
	n_cur_rows = data.shape[0]

	n_new_cols = n_cur_cols * sequence_length
	n_new_rows = n_cur_rows / sequence_length
	sequences = np.reshape(data, (n_new_rows, n_new_cols))
	return sequences

def create_samples_from_feature_files(pairs, sequence_length):
	"""
	:description: main function that takes a list of tuples (pairs) of filenames and returns the data from those files in the form of data samples. 
	"""
	samples = []
	for pair in pairs:
		p1_data = read_data(pair[0])
		p2_data = read_data(pair[1])

		# if either file is empty then ignore this pair
		if not p1_data or not p2_data:
			continue

		p1_sequence_data = group_single_timestep_data_into_sequences(p1_data, sequence_length)
		p2_sequence_data = group_single_timestep_data_into_sequences(p2_data, sequence_length)

		sample = np.column_stack((p1_sequence_data, p2_sequence_data))
		samples.append(sample)
	return samples

if __name__ == '__main__':
	# read in data as a squence of frames in the format [binary_smile_person_1, binary_smile_person_2]
	features_directory = '/Users/wulfe/Dropbox/Start/smile/data/smile_assessment'
	filepaths = load_filenames_from_directory(features_directory)
	pairs = get_filepath_pairs_by_interaction(filepaths)
	sequence_length = 8 
	sample_data = create_samples_from_feature_files(pairs, sequence_length)
	output_filename = '/Users/wulfe/Dropbox/Start/smile/data/smiles_by_frame_as_sample_data/smile_sample.csv'
	write_data_to_file(sample_data, output_filename)
