import random
import sys
import six
from datetime import datetime
import json
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import time
import string
class Producer(object):
    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_symbol):
        k=0
        printable = set(string.printable)
        while True:
            with open('tweets_ds.json', 'r') as json_file:
                for line in json_file:
                    #try:
                    data = json.loads(line)
                    userid_field = data['user']['id']
                    text_field_a = (data['text']).encode('utf-8')
                    text_field = filter(lambda x: x in printable, text_field_a)
                    createdat_field = data['created_at']
                    tweetid_field = data['id']
                    str_fmt = "{};{};{};{};{}"
                    message_info = str_fmt.format(source_symbol,
                                                  userid_field,
                                                  text_field,
                                                  createdat_field,
                                                  tweetid_field)
                    if k%10 == 0:
                        time.sleep(2)
                    self.producer.send_messages('tweets1', source_symbol, message_info)
                    k=k+1
                    #except:
                        #pass

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
