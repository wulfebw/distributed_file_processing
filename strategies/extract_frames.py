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
