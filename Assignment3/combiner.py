import os
def combine(input_dir, merged_output_path):
    with open(merged_output_path, 'w') as merged_file:
        for root, dirs, files in os.walk(input_dir):
            for file in files:
                if file == 'output.txt':
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r') as input_file:
                        merged_file.write(input_file.read() + '\n')
         