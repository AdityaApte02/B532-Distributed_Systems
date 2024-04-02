import os
def combine(input_dir, merged_output_path):
    with open(merged_output_path, 'w') as merged_file:
        for root, dirs, files in os.walk(input_dir):
            for file in files:
                if file == 'output.txt':
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r') as input_file:
                        merged_file.write(input_file.read() + '\n')
                        
                        
                        
def cleanUp(testCase):
    home_directory = f"tests/{testCase}/home"
    for dir_name in os.listdir(home_directory):
        cur_dir = os.path.join(home_directory, dir_name)
        for directory_name in os.listdir(cur_dir):
            directory_path = os.path.join(cur_dir, directory_name)
            if os.path.isdir(directory_path):  
                if "mapper" in directory_name :
                    output_file_path = os.path.join(directory_path, "output.txt")
                    if os.path.exists(output_file_path): 
                        os.remove(output_file_path) 
                         
                elif "reducer" in directory_name:
                    input_file_path = os.path.join(directory_path, "input.txt")
                    if os.path.exists(input_file_path):  
                        os.remove(input_file_path) 
                    
                    output_file_path = os.path.join(directory_path, "output.txt")
                    if os.path.exists(output_file_path):  
                        os.remove(output_file_path)  
    print("Files deleted successfully.")    
