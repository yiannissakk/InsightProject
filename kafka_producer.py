import random
import sys
import six
import json
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import time
import string
import urllib

class Producer(object):
    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_symbol):
        
        #read from S3 bucket
        opener = urllib.URLopener()
        myurl = "https://s3-us-west-2.amazonaws.com/timo-twitter-data/2016-02-08-11-57_tweets.txt"
        myfile = opener.open(myurl)
        k=0
        
        
        printable = set(string.printable) #set of ascii (printable characters)
        while True:
            for line in myfile:
                try:
                    #filter tweets: keep fields of interest
                    data = json.loads(line)
                    userid_field = data['user']['id']
                    username_field = data['user']['screen_name']
                    text_field_a = (data['text']).encode('utf-8')
                    text_field = filter(lambda x: x in printable, text_field_a) #remove any non ascii text
                    createdat_field = data['created_at']
                    tweetid_field = data['id']
                    str_fmt = "{};{};{};{};{};{}"
                    message_info = str_fmt.format(source_symbol,
                                                  userid_field,
                                                  text_field,
                                                  createdat_field,
                                                  tweetid_field,
                                                  username_field)
                    if k%10 == 0:
                        time.sleep(2)
                        
                    #the producers queue messages in the form (topic, producer#, message)   
                    self.producer.send_messages('tweets1', source_symbol, message_info)
                    k=k+1
                except:
                    pass

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
