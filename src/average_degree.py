# Import the necessary methods from library
import json
from datetime import datetime
from datetime import timedelta
from email.utils import parsedate_tz, mktime_tz
import os


# Find the directory of README.md
directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Find the input directory
file_path = os.path.join(directory, 'tweet_input/tweets.txt')
# Set the output directory
output_path = os.path.join(directory, 'tweet_output/output.txt')


# ReadData is the original 'tweet.txt' file, which can be read line by line
# Used to create file_read
class ReadData(object):
    def __init__(self, my_file):
        self.my_file = my_file

    def read_file(self):
        with open(self.my_file) as data_file:
            data_file_line = data_file.readlines()
        return data_file_line


# ExtractData is file_read, where the created-time and hashtags information can be extracted
# Used to create file_extracted (store time and hashtags information)
class ExtractData(object):
    def __init__(self, my_file):
        self.my_file = my_file

    def extract_file(self):
        extracted_file = {}
        i = 0  # Used to count actual JSON messages (with time and hashtags information)
        # j=0  # Used to count API messages about connection and rate limits (without time and hashtags information)
        for line in self.my_file:
            temp = json.loads(line)  # read data line by line
            if 'created_at' in temp.keys():  # select the actual JSON messages
                temp_time = temp['created_at']  # extract time
                temp_tag = temp['entities']['hashtags']  # extract hashtags
                # transfer time string to datatime frame
                # temp_time = datetime.strptime(temp_time, '%a %b %d %H:%M:%S %z %Y')
                # strptime is Python platform only, do not use the above query if run by shell
                temp_time = datetime(1970, 1, 1) + timedelta(seconds=mktime_tz(parsedate_tz(temp_time)))
                # store time, hashtags information,and later extracted hashtag words to new file
                extracted_file[i] = [temp_time, temp_tag, []]
                # extract the hashtag words
                if temp_tag:
                    for tag in temp_tag:
                        extracted_file[i][2].append(tag['text'])
                else:  # no hashtags
                    pass
                i += 1
            else:
                # these messages are Twitter API resulting from the rate-limit
                # can be stored in apifile for future uses
                # here we remove these messages from dataset
                pass
                # apifile[j] = temp
                # j += 1
        return extracted_file


# SortData is file_extracted, which can be sorted by create time
# Used to create file_sorted to rank tweets in time (in case that there are tweets which arrive out of order)
class SortData(object):
    def __init__(self, my_file):
        self.my_file = my_file

    def sort_file(self):
        # sorted_file is a sorted list
        sorted_file = sorted(self.my_file.values(), key=lambda x: x[0])
        # delete the original hashtags information
        for item in sorted_file:
            del(item[1])
        # sorted_file[0] is time
        # sorted_file[1] is hashtag words
        return sorted_file


# Graph if Twitter Hashtag Graph with list of nodes and edges
# Used to compute the average_degree
class Graph(object):
    def __init__(self, hashtags, nodes, edges):
        self.hashtags = hashtags
        self.nodes = nodes
        self.edges = edges

    def count_average_degree(self):
        no_of_edges = len(self.edges)
        no_of_nodes = len(self.nodes)
        if no_of_edges == 0:
            return 0.00
        else:
            average_degree = float(no_of_edges)*2 / no_of_nodes
            return average_degree

    # Update the Twitter Hashtag Graph with the updated hashtags
    def add_graph(self):
        # the first time run this function, nodes and edges are null []'s
        # for iterations, nodes and edges will be updated
        # if the start index doesn't change, use added_data's hashtags with previous nodes and edges
        # otherwise, use new one-minute-data's hashtags with nodes and edges being []'s
        for item in self.hashtags:
            temp_n = len(item)
            # update nodes
            for text in item:
                if text not in self.nodes:
                    self.nodes.append(text)
            # update edges
            for i in range(0, temp_n - 1):
                for j in range(i + 1, temp_n):
                    if [item[i], item[j]] not in self.edges and [item[j], item[i]] not in self.edges \
                            and item[i] != item[j]:
                        self.edges.append([item[i], item[j]])
        return [self.nodes, self.edges]


# Create dataset one_minute_data
# Used to updating dataset within the 60 second window
def data_in_minute(whole_file, last_start_index, last_end_index, no_of_tweets):
    # whole_file is the sorted real tweets data
    # last_start_index and last_end_index is the bound indices of the data in the last 60 second window
    if last_end_index + 1 < no_of_tweets:  # check if there IS a new tweet
        new_end_index = last_end_index + 1  # a new tweet comes in
    else:
        return 'No more tweets!'
    j = 0
    # delete the out-of-60s-time tweets
    while whole_file[new_end_index][0] - whole_file[last_start_index + j][0] >= timedelta(minutes=1):
        j += 1
    new_start_index = last_start_index + j
    # create the updated 60s window data using the new start & end index
    one_minute_data = whole_file[new_start_index:(new_end_index + 1)]
    # create a dataset that contains only the new_added tweets
    added_data = whole_file[(last_end_index + 1):(new_end_index + 1)]
    return [one_minute_data, new_start_index, new_end_index, added_data]


# The tweets with no hashtag or only one hashtag won't generate any edges and nodes to Hashtag Graph
# Filter the tweets, to select those who have more than one hashtags, and store them in 'hashtags'
def filter_hashtag(my_file):
    # my_file is a partial of file_sorted, which is a list of [time,[hashtags]]
    hashtags = []  # will be returned as a list of all meaningful hashtags in my_file
    i = 0
    for item in my_file:
        if len(item[1]) > 1:
            hashtags.append(item[1])
            i += 1
    return hashtags


def main_tweet(input_file_path, output_file_path):
    file_read = ReadData(input_file_path).read_file()
    file_extracted = ExtractData(file_read).extract_file()
    file_sorted = SortData(file_extracted).sort_file()
    no_of_tweets = len(file_sorted)
    updated_start_index = 0
    updated_end_index = -1
    nodes = []
    edges = []
    window_data_result = data_in_minute(file_sorted, updated_start_index, updated_end_index, no_of_tweets)
    with open(output_file_path, 'w') as output_file:
        while type(window_data_result) == list:
            updated_data = window_data_result[0]  # all the data in the new 60s time window
            start_index = updated_start_index
            updated_start_index = window_data_result[1]  # the first index of one_minute_data
            updated_end_index = window_data_result[2]  # the last index of one_minute_data
            data_added = window_data_result[3]  # the new data added to the one_minute_data
            if start_index == updated_start_index:
                data_hashtag = filter_hashtag(data_added)
                graph_result = Graph(data_hashtag, nodes, edges)
            else:
                data_hashtag = filter_hashtag(updated_data)
                graph_result = Graph(data_hashtag, [], [])
            nodes = graph_result.add_graph()[0]
            edges = graph_result.add_graph()[1]
            output_file.write('%.2f\n' % graph_result.count_average_degree())
            window_data_result = data_in_minute(file_sorted, updated_start_index, updated_end_index, no_of_tweets)
    output_file.close()

if __name__ == '__main__':
    main_tweet(file_path, output_path)
