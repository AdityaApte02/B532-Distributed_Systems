import unittest
import os

class TestOutputFiles(unittest.TestCase):
    def test_files_within_subfolders_are_identical(self):
        test_outputs_folder = "./TestOutputs" 
        lines_to_compare = 5
        subfolders = [f.path for f in os.scandir(test_outputs_folder) if f.is_dir()]
        for subfolder in subfolders:

            with self.subTest(subfolder=subfolder):
                files_in_subfolder = os.listdir(subfolder)
                self.assertGreater(len(files_in_subfolder), 0)
                first_file_path = os.path.join(subfolder, files_in_subfolder[0])

                for file_name in files_in_subfolder[1:]:
                    file_path = os.path.join(subfolder, file_name)

                    with open(first_file_path, 'r') as first_file, open(file_path, 'r') as current_file:
                        first_file_lines = first_file.readlines()[:lines_to_compare]
                        current_file_lines = current_file.readlines()[:lines_to_compare]
                        
                        self.assertEqual(
                            first_file_lines, current_file_lines,
                            f"Files {files_in_subfolder[0]} and {file_name} in {subfolder} are not identical."
                        )

    
