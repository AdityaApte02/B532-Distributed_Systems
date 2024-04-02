import re
import time
import sys
def map_task(text, id):
    document = "D"+id[-1]
    cleaned_text = re.sub(r'[^\w\s]', '', text)
    lines = cleaned_text.split(" ")
    for word in lines:
        time.sleep(0.05)
        print(word,f"\t{document}")

if __name__ == '__main__':
    data = sys.stdin.read()
    map_task(data, sys.argv[1])