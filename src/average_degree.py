import json
from datetime import datetime
from datetime import timedelta
from email.utils import parsedate_tz,mktime_tz
import sys
import os


direct = os.getcwd()
print(__file__)
print(os.path.join(os.path.dirname(__file__),'tweets.txt'))
#tweet_file = os.
directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
file_path = os.path.join(directory,'tweet_input/tweets.txt')
output_path = os.path.join(directory,'tweet_output/output.txt')



class Read_data(object):
# read the data 'tweet.txt' to data_file
    def __init__(self,my_file):
        self.my_file = my_file

    def read_file(self):
        with open(self.my_file) as data_file:
            data_file_line = data_file.readlines()
        return data_file_line

class Extract_data(object):
# store time and hashtags information in new file
    def __init__(self,my_file):
        self.my_file = my_file

    def extract_file(self):
        extracted_file = {}
        i = 0
        # j=0
        for line in self.my_file:
            temp = json.loads(line)
            if 'created_at' in temp.keys(): # select the real tweet data
                temp_time = temp['created_at'] # extract time
                temp_tag = temp['entities']['hashtags'] # extract tags
                # transfer time string to datatime frame
                # temp_time = datetime.strptime(temp_time, '%a %b %d %H:%M:%S %z %Y')
                temp_time = datetime(1970, 1, 1) + timedelta(seconds=mktime_tz(parsedate_tz(temp_time)))
                # store time,hashtags,and later extracted hashtags to new file
                extracted_file[i] = [temp_time, temp_tag, []]
                # extract the hashtag words
                if temp_tag != []:
                    for tag in temp_tag:
                        extracted_file[i][2].append(tag['text'])
                else: # no hashtags
                    pass
                i += 1
            else:
                pass
                # apifile[j] = temp # store the {limit} api message
                # j += 1
        return(extracted_file)

class Sort_data(object):
    def __init__(self,my_file):
        self.my_file = my_file

    # sort tweet_file in time order
    def sort_file(self):
        sorted_file = sorted(self.my_file.values(),key = lambda x: x[0])
        # sorted_file is a sorted list
        # delete the original hashtags information
        for item in sorted_file:
            del(item[1])
        # sorted_file[0] is time
        # sorted_file[1] is hashtags text
        return(sorted_file)


#file_read = Read_data('tweets.txt').read_file()
#file_extracted = Extract_data(file_read).extract_file()
#file_sorted = Sort_data(file_extracted).sort_file()
#N = len(file_sorted)
# print(N)


def data_in_minute(whole_file,last_start_index,last_end_index,N):
    # a new tweet comes in
    if last_end_index + 1 < N:
        #\ and whole_file[last_end_index + i + 1][0] == whole_file[last_end_index + 1][0]:
        # check if there is other tweet comes in at the same time
        new_end_index = last_end_index + 1
    else:
        return 'No more tweets!'
    j = 0
    # delete the out-of-time tweets
    while whole_file[new_end_index][0] - whole_file[last_start_index + j][0] >= timedelta(minutes = 1):
        j += 1
    new_start_index = last_start_index + j
    # create the 60s dataset using the new start & end index
    one_minute_data = whole_file[new_start_index:(new_end_index + 1)]
    added_data = whole_file[last_end_index:(new_end_index + 1)]
    removed_data = whole_file[last_start_index:(new_start_index + 1)]
    return [one_minute_data,new_start_index,new_end_index,added_data]
"""
start_index = 9290
end_index = 9293
result = data_in_minute(file_sorted,start_index,end_index)
timestamp_data = result[0] # all the data in the new 60s time window
start_index = result[1] # the first index of timestamp_data
end_index = result[2] # the last index of timestamp_data
data_add = result[3] # the new data added to the timestamp_data
print(timestamp_data)
print(start_index)
print(end_index)
"""
def filter_hashtag(my_file):
    # filter the tweets, remain those who have more than one hashtags, stored the hashtags in 'hashtags'
    hashtags = []
    # my_file is partial tweet_file, which is a list of [time,[hashtags]]
    i = 0
    for item in my_file:
        if len(item) == 1:
            print(item)
        elif len(item[1]) > 1:
            hashtags.append(item[1])
            i += 1
    return hashtags
"""
data_hashtag = filter_hashtag(timestamp_data)
for item in data_hashtag:
    print(item)
"""
def Add_graph(hashtags,nodes,edges):
    # the first time run this function, nodes and edges are null []'s
    # for iterations, nodes and edges will be updated as
    # if the start index doesn't change, use added_data's hashtags, and nodes and edges are original values
    # otherwise, use new timestamp_data's hashtags, and nodes and edges are []'s
    for item in hashtags:
        temp_n = len(item)
        for text in item:
            if text not in nodes:
                nodes.append(text)
        for i in range(0, temp_n - 1):
            for j in range(i + 1, temp_n):
                if [item[i],item[j]] not in edges and [item[j],item[i]] not in edges \
                        and item[i] != item[j]:
                    edges.append([item[i], item[j]])
    return [nodes,edges]
"""
result2 = Add_graph(data_hashtag,[],[])
nodes = result2[0]
edges = result2[1]
print(nodes)
print(edges)
"""




class Graph(object):
    def __init__(self,nodes,edges):
        self.nodes = nodes
        self.edges = edges
    def Average_degree(self):
        no_edges = len(self.edges)
        no_nodes = len(self.nodes)
        if no_edges == 0:
            return 0.00
        else:
            average_degree = float(no_edges)*2 / no_nodes
            return average_degree

# ourgraph = Graph(nodes,edges)
# print(ourgraph.Average_degree())

def main_tweet(file_path,output_path):
    file_read = Read_data(file_path).read_file()
    file_extracted = Extract_data(file_read).extract_file()
    file_sorted = Sort_data(file_extracted).sort_file()
    N = len(file_sorted)
    window_data_result = []
    updated_start_index = 0
    updated_end_index = 0
    nodes = []
    edges = []
    window_data_result = data_in_minute(file_sorted, updated_start_index, updated_end_index,N)
    with open(output_path,'w') as output_file:
        while type(window_data_result) == list:
            updated_data = window_data_result[0]  # all the data in the new 60s time window
            start_index = updated_start_index
            updated_start_index = window_data_result[1]  # the first index of timestamp_data
            updated_end_index = window_data_result[2]  # the last index of timestamp_data
            data_added = window_data_result[3]  # the new data added to the timestamp_data
            if start_index == updated_start_index:
                data_hashtag = filter_hashtag(data_added)
            else:
                data_hashtag = filter_hashtag(updated_data)
            graph_result = Add_graph(data_hashtag,nodes,edges)
            nodes = graph_result[0]
            edges = graph_result[1]
            ourgraph = Graph(nodes, edges)
            output_file.write('%.2f\n' %ourgraph.Average_degree())
            window_data_result = data_in_minute(file_sorted, updated_start_index, updated_end_index,N)
    #print(window_data_result)
    output_file.close()

if __name__ == '__main__':
    main_tweet(file_path,output_path)
