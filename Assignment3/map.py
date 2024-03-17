import re
import time
import sys
def map_task(text):
    cleaned_text = re.sub(r'[^\w\s]', '', text)
    lines = cleaned_text.split(" ")
    for word in lines:
        time.sleep(0.01)
        print(word,"\t1")
    
    # word_counts = {}
    # words = cleaned_text.split()
    # for word in words:
    #     word_counts[word] = word_counts.get(word, 0) + 1
        
    # return word_counts


if __name__ == '__main__':
    data = sys.stdin.read()
    map_task(data)