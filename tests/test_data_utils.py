import sys
sys.path.append('/Users/wulfe/Dropbox/Start/scripts/')
from distributed_file_processing import data_utils

import unittest
import numpy as np

class TestDataUtils(unittest.TestCase):

	""" get_filepath_pairs_by_interaction tests """
	def test_get_filepath_pairs_by_interaction_no_pairs(self):
		filepaths = []
		actual = data_utils.get_filepath_pairs_by_interaction(filepaths)
		expected = []
		self.assertEquals(actual, expected)

	def test_get_filepath_pairs_by_interaction_matched_pairs_exist(self):
		filepaths = ['/test/test/0209_f01_m01.csv',
					'/test/test/0209_m01_f01.csv',
					'/test/test/0209_f01_m02.csv',
					'/test/test/0209_m02_f01.csv']
		actual = data_utils.get_filepath_pairs_by_interaction(filepaths)
		expected = [('/test/test/0209_f01_m01.csv', '/test/test/0209_m01_f01.csv'),
					('/test/test/0209_f01_m02.csv', '/test/test/0209_m02_f01.csv')]
		self.assertItemsEqual(actual, expected)

	def test_get_filepath_pairs_by_interaction_missing_match_for_file(self):
		filepaths = ['/test/test/0209_f01_m01.csv',
					'/test/test/0209_m01_f01.csv',
					'/test/test/0209_f01_m02.csv',
					'/test/test/0209_m02_f01.csv',
					'/test/test/0209_m02_f02.csv']
		actual = data_utils.get_filepath_pairs_by_interaction(filepaths)
		expected = [('/test/test/0209_f01_m01.csv', '/test/test/0209_m01_f01.csv'),
					('/test/test/0209_f01_m02.csv', '/test/test/0209_m02_f01.csv')]
		self.assertItemsEqual(actual, expected)

	def test_get_filepath_pairs_by_interaction_no_pairs_repeat_files(self):
		filepaths = ['/test/test/0209_f01_m01.csv',
					'/test/test/0209_m02_f01.csv',
					'/test/test/0209_f01_m01.csv',
					'/test/test/0209_m02_f01.csv']
		actual = data_utils.get_filepath_pairs_by_interaction(filepaths)
		expected = []
		self.assertItemsEqual(actual, expected)

	def test_get_filepath_pairs_by_interaction_matched_pairs_exist_w_repeats(self):
		filepaths = ['/test/test/0209_f01_m01.csv',
					'/test/test/0209_m01_f01.csv',
					'/test/test/0209_f01_m02.csv',
					'/test/test/0209_m02_f01.csv',
					'/test/test/0209_f01_m01.csv']
		actual = data_utils.get_filepath_pairs_by_interaction(filepaths)
		expected = [('/test/test/0209_f01_m01.csv', '/test/test/0209_m01_f01.csv'),
					('/test/test/0209_f01_m02.csv', '/test/test/0209_m02_f01.csv')]
		self.assertItemsEqual(actual, expected)

	""" get_interaction_name tests """
	def test_get_interaction_name_male_first(self):
		filepath = '/test/test/0209_m01_f02.csv'
		actual = data_utils.get_interaction_name(filepath)
		expected = '0209_m01_f02'
		self.assertEquals(actual, expected)

	def test_get_interaction_name_female_first(self):
		filepath = '/test/test/0209_f01_m02.csv'
		actual = data_utils.get_interaction_name(filepath)
		expected = '0209_m02_f01'
		self.assertEquals(actual, expected)

	def test_get_interaction_name_empty_filepath(self):
		filepath = ''
		actual = data_utils.get_interaction_name(filepath)
		expected = ''
		self.assertEquals(actual, expected)

	""" group_single_timestep_data_into_sequences tests """
	def test_group_single_timestep_data_into_sequences_empty_data(self):
		sequence = []
		sequence_length = 2
		with self.assertRaises(IndexError):
			data_utils.group_single_timestep_data_into_sequences(sequence, sequence_length)

	def test_group_single_timestep_data_into_sequences_single_item_timestep(self):
		sequence = [[1],[2],[3],[4]]
		sequence_length = 2
		actual = data_utils.group_single_timestep_data_into_sequences(sequence, sequence_length)
		expected = np.array([[1,2], [3,4]])
		self.assertTrue(np.array_equal(actual, expected))

	def test_group_single_timestep_data_into_sequences_multi_item_timestep(self):
		sequence = [[1,2],[2,3],[3,4],[4,5]]
		sequence_length = 2
		actual = data_utils.group_single_timestep_data_into_sequences(sequence, sequence_length)
		expected = np.array([[1,2,2,3], [3,4,4,5]])
		self.assertTrue(np.array_equal(actual, expected))

	def test_group_single_timestep_data_into_sequences_multi_item_timestep_odd(self):
		sequence = [[1,2],[2,3],[3,4],[4,5],[5,6]]
		sequence_length = 2
		actual = data_utils.group_single_timestep_data_into_sequences(sequence, sequence_length)
		expected = np.array([[1,2,2,3], [3,4,4,5]])
		self.assertTrue(np.array_equal(actual, expected))

	def test_group_single_timestep_data_into_sequences_multi_item_timestep_larger_sequence(self):
		sequence = [[1,2],[2,3],[3,4],[4,5]]
		sequence_length = 4
		actual = data_utils.group_single_timestep_data_into_sequences(sequence, sequence_length)
		expected = np.array([[1,2,2,3,3,4,4,5]])
		self.assertTrue(np.array_equal(actual, expected))

	""" column_stack_with_concatenate tests """


	""" create_sample_from_features """
	def test_create_sample_from_features_empty_data(self):
		data_1 = []
		data_2 = []
		sequence_length = 2
		with self.assertRaises(IndexError):
			actual = data_utils.create_sample_from_features(data_1, data_2, sequence_length)

	def test_create_sample_from_features_single_col_feature(self):
		data_1 = [[1],[2],[3],[4]]
		data_2 = [[5],[6],[7],[8]]
		sequence_length = 2
		actual = data_utils.create_sample_from_features(data_1, data_2, sequence_length)
		expected = [[1,2,5,6],[3,4,7,8]]
		self.assertTrue(np.array_equal(actual, expected))

	def test_create_sample_from_features_single_col_feature_increase_seq_len(self):
		data_1 = [[1],[2],[3],[4]]
		data_2 = [[5],[6],[7],[8]]
		sequence_length = 4
		actual = data_utils.create_sample_from_features(data_1, data_2, sequence_length)
		expected = [[1,2,3,4,5,6,7,8]]
		self.assertTrue(np.array_equal(actual, expected))

	def test_create_sample_from_features_multi_col_feature(self):
		data_1 = [[1,2],[2,3],[3,4],[4,5]]
		data_2 = [[5,6],[6,7],[7,8],[8,9]]
		sequence_length = 2
		actual = data_utils.create_sample_from_features(data_1, data_2, sequence_length)
		expected = [[1,2,2,3,5,6,6,7],[3,4,4,5,7,8,8,9]]
		self.assertTrue(np.array_equal(actual, expected))



if __name__ == '__main__':
	suite = unittest.TestLoader().loadTestsFromTestCase(TestDataUtils)
	unittest.TextTestRunner(verbosity=2).run(suite)