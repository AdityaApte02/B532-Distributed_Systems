import re
import time
import sys
def map_task(text, id):
    cleaned_text = re.sub(r'[^\w\s]', '', text)
    lines = cleaned_text.split(" ")
    for word in lines:
        time.sleep(0.05)
        print(word,"\t1")

if __name__ == '__main__':
    data = sys.stdin.read()
    map_task(data, sys.argv[1])