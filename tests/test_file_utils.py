import sys
sys.path.append('/Users/wulfe/Dropbox/Start/scripts/')
from distributed_file_processing import file_utils

import unittest
import numpy as np

class TestFileUtils(unittest.TestCase):

	""" read_data tests """
	def test_read_data_no_filepath(self):
		filepath = ''
		with self.assertRaises(IOError):
			file_utils.read_data(filepath)


	""" wrtie data tests """

	""" load_filenames_from_directory tests """


if __name__ == '__main__':
	suite = unittest.TestLoader().loadTestsFromTestCase(TestFileUtils)
	unittest.TextTestRunner(verbosity=2).run(suite)