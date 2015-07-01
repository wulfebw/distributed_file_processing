import sys
sys.path.append('/Users/wulfe/Dropbox/Start/scripts/')
from distributed_file_processing.strategies import extract_visual_features

import unittest
import numpy as np

class TestExtractVisualFeatures(unittest.TestCase):
	
	""" extract_frame_number_from_path tests """
	def test_extract_frame_number_from_filepath_no_name(self):
		filepath = ''
		with self.assertRaises(ValueError):
			actual = extract_visual_features.extract_frame_number_from_filepath(filepath)

	def test_extract_frame_number_from_filepath_valid_name(self):
		filepath = '/test/test/0209_f01_m01_3004.jpg'
		actual = extract_visual_features.extract_frame_number_from_filepath(filepath)
		expected = 3004
		self.assertEqual(actual, expected)

	def test_extract_frame_number_from_filepath_invalid_name(self):
		filepath = '/test/test/0209_f01_m01.jpg'
		with self.assertRaises(ValueError):
			actual = extract_visual_features.extract_frame_number_from_filepath(filepath)

	""" sort_img_paths tests """
	def test_sort_img_paths_valid_basic_sort(self):
		img_paths = ['/test/test/0209_f01_m01_400.jpg', '/test/test/0209_f01_m01_3000.jpg', '/test/test/0209_f01_m01_24.jpg']
		actual = extract_visual_features.sort_img_paths(img_paths)
		print actual
		expected = ['/test/test/0209_f01_m01_24.jpg', '/test/test/0209_f01_m01_400.jpg','/test/test/0209_f01_m01_3000.jpg']
		self.assertEquals(actual, expected)

	def test_sort_img_paths_valid_complex_string_sort(self):
		img_paths = ['/test/test/0209_f01_m01_1929.jpg', '/test/test/0209_f01_m01_192.jpg']
		actual = extract_visual_features.sort_img_paths(img_paths)
		expected = ['/test/test/0209_f01_m01_192.jpg', '/test/test/0209_f01_m01_1929.jpg']
		self.assertEquals(actual, expected)

	def test_sort_img_paths_valid_complex_string_sort_zero(self):
		img_paths = ['/test/test/0209_f01_m01_1920.jpg', '/test/test/0209_f01_m01_192.jpg']
		actual = extract_visual_features.sort_img_paths(img_paths)
		expected = ['/test/test/0209_f01_m01_192.jpg', '/test/test/0209_f01_m01_1920.jpg']
		self.assertEquals(actual, expected)

	""" crop_image tests """
	def test_crop_image_valid_central_crop(self):
		pass

	""" extract_visual_features test """
	def test_extract_visual_features_single_valid_extraction(self):
		pass

if __name__ == '__main__':
	suite = unittest.TestLoader().loadTestsFromTestCase(TestExtractVisualFeatures)
	unittest.TextTestRunner(verbosity=2).run(suite)

