import json
from master import Master

if __name__ == "__main__":
    config_file_path = './config.json'    
    with open(config_file_path, 'r') as file:
         data = json.load(file)
         master = Master(data["MasterHost"], data["MasterPort"],data["mappers"], data["reducers"], data["map_function"], data["reduce_function"])
         master.run()
         
         