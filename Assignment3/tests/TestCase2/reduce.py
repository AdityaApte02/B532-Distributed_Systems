# import sys
# import ast
# def reduce_task(text):
#     lines = text.split('\n')
#     word_dict = {}
#     for line in lines:
#         if len(line.split("\t")) == 2:
#             word, documents = line.split("\t")
#             document = ast.literal_eval(documents)[0]
#             if word not in word_dict.keys():
#                 word_dict[word] = [document]
#             else:
#                 if document not in word_dict[word]:
#                     word_dict[word].append(document)
                
#     for word in word_dict.keys():
#         print(word, "\t", word_dict[word])
        
        
# if __name__ =="__main__":
#     data = sys.stdin.read()
#     reduce_task(data)
import sys
import ast
def reduce_task(text):
    lines = text.split('\n')
    word_dict = {}
    for line in lines:
        if len(line.split("\t")) == 2:
            word, documents = line.split("\t")
            document = ast.literal_eval(documents.replace("'",""))
            if word not in word_dict:
                word_dict[word]  = []
            if document[0] not in word_dict[word]:
                word_dict[word].append(document[0]) 
    for word in word_dict.keys():
        print(word,"\t",word_dict[word])
        
        
if __name__ =="__main__":
    data = sys.stdin.read()
    reduce_task(data)