from pymemcache.client.base import Client
import time
import json
import sys

# Example usage
client = Client(('127.0.0.1', 8112))

commands = None
with open(sys.argv[1], "r") as file:
    commands = json.load(file)


result = []
for comm in commands:
    if comm["type"] == "set":
        result.append(client.raw_command(
            "set " + comm["key"] + " " + comm["chunkSize"] + " noreply " + comm["value"] + " \n"))
    else:
        result.append(client.raw_command("get " + comm["key"], end_tokens="END\r\n"))
    time.sleep(1)

path = (
    "./Outputs/output"
    +sys.argv[1][-6]
    + ".txt"
)
               
with open(path, 'w', encoding='utf-8') as file:
    for data in result:
        text_decoded = data.decode("utf-8")
        file.write("=======================\n" + text_decoded+ "\n")

