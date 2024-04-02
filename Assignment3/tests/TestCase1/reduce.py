# import sys
# import os
# import json
# import ast
# def reduce_task(text):
#     lines = text.split('\n')
#     word_dict = {}
#     for line in lines:
#         if len(line.split()) == 2:
#             word, counts = line.split()
#             counts = [element.strip("'") for element in counts]
#             count = sum(list(map(int, counts)))
#             if word not in word_dict.keys():
#                 word_dict[word] = int(count)
#             else:
#                 word_dict[word] = word_dict[word] + int(count)
#     for word in word_dict.keys():
#         print(word, "\t", word_dict[word])
        
        
# if __name__ == "__main__":
#     data = sys.stdin.read()
#     reduce_task(data)
import sys
import os
import json
import ast
def reduce_task(text):
    lines = text.split('\n')
    word_dict = {}
    for line in lines:
        if len(line.split("\t")) == 2:
            word, counts = line.split("\t")
            counts = ast.literal_eval(counts.replace("'",""))
            count = sum(list(map(int, counts)))
            if word not in word_dict.keys():
                word_dict[word] = int(count)
            else:
                word_dict[word] = word_dict[word] + int(count)
    for word in word_dict.keys():
        print(word, "\t", word_dict[word])
        
        
if __name__ == "__main__":
    data = sys.stdin.read()
    reduce_task(data)