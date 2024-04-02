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
                        
                    output1_file_path = os.path.join(directory_path, "output1.txt")
                    if os.path.exists(output1_file_path): 
                        os.remove(output1_file_path) 
                         
                elif "reducer" in directory_name:
                    input_file_path = os.path.join(directory_path, "input.txt")
                    if os.path.exists(input_file_path):  
                        os.remove(input_file_path) 
                    
                    output_file_path = os.path.join(directory_path, "output.txt")
                    if os.path.exists(output_file_path):  
                        os.remove(output_file_path)  
    print("Files deleted successfully.")
    
    
def createFiles(testCase, num_mappers, num_reducers):
    home_dir = f'tests/{testCase}/home'
    map_dir = os.path.join(home_dir, "mappers")
    if not os.path.exists(map_dir):
        os.makedirs(map_dir)
    red_dir = os.path.join(home_dir, "reducers")
    if not os.path.exists(red_dir):
        os.makedirs(red_dir)
        
    for i in range(1, num_mappers + 1):
        mapper_dir = os.path.join(map_dir, f'mapper{i}')
        output_file_path = os.path.join(mapper_dir, 'output.txt')
        if not os.path.exists(mapper_dir):
            os.makedirs(mapper_dir)
        with open(output_file_path, 'w') as output_file:
            output_file.write('')   
            
    for i in range(1, num_reducers + 1):
        reducer_dir = os.path.join(red_dir, f'reducer{i}')
        if not os.path.exists(reducer_dir):
            os.makedirs(reducer_dir) 
        input_file_path = os.path.join(reducer_dir, 'input.txt')
        with open(input_file_path, 'w') as input_file:
            input_file.write('')
            
        output_file_path = os.path.join(reducer_dir, 'output.txt')
        with open(output_file_path, 'w') as output_file:
            output_file.write('')

         