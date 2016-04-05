# InsightData-CodingChallenge
Calculate the average degree of a vertex in a Twitter hashtag graph for the last 60 seconds, and update this each time a new tweet appears. (Calculate the average degree over a 60-second sliding window.)

**Modules** I used in my Python coding: datetime, json, os, and email.utils

I sort the tweets.txt file first, and then keep updating the dataset within 60s window when a new tweet comes in. Then the Twitter Hashtag Graph would be generated according to the 60s-dataset, and average degree will be calculated.

I tested my code regarding each specific class or function, and passed the 2-tweet test and the data-gen-tweet test.
I think I can treat these tweets as queue, but the running time of sorting and generating graphs may be able to be reduced. Especially, I sorted the whole file before calculating average degree. However, as the file is almost in order of time, I think there might be quicker solutions. Very welcome to discuss with me!
