import sys
import os
def reduce_task(text):
    lines = text.split('\n')
    word_dict = {}
    for line in lines:
        if len(line.split()) == 2:
            word, count = line.split()
            if word not in word_dict.keys():
                word_dict[word] = 1
                
            else:
                word_dict[word] = word_dict[word] + 1
    for word in word_dict.keys():
        print(word, "\t", word_dict[word])
        
        
if __name__ == "__main__":
    data = sys.stdin.read()
    reduce_task(data)