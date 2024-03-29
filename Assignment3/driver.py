import json
import os
import sys
from master import Master

if __name__ == "__main__":
    testcase_num = sys.argv[1]
    testcase = 'TestCase'+str(testcase_num)
    test_dir = 'tests'
    directory_path = os.path.join(test_dir, f"TestCase{testcase_num}")
    if os.path.isdir(directory_path):
        config_file_path = os.path.join(directory_path, 'config.json')
        if os.path.exists(config_file_path):
            with open(config_file_path, 'r') as file:
                data = json.load(file)
                master = Master(data["MasterHost"], data["MasterPort"],data["mappers"], data["reducers"], data["map_function"], data["reduce_function"],testcase, data["clear"])
                master.run()
                    
                    
         
         