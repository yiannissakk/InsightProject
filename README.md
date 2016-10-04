# TweetSearch: A Twitter Search Engine

TweetSearch is an app that lets users search for tweets given a specific keyword and sentiment.

TweetSearch is powered by:

- Kafka
- Spark Streaming
- Elasticsearch
- Flask

# Pipeline

All of the technologies listed above are connected as follows

![alt text](https://github.com/yiannissakk/InsightProject/frontEnd/static/pipeline.png)

# Data Ingestion

The Kafka producers read from an S3 tweet data dump. The data dump contains 150GB worth of tweets. Before the queues receive the data, the producers filter out all the fields of the tweets that are of no use to TweetSearch. The fields that are kept are {text, user_id, username, date, tweet_id}. (the code for the producer can be found in producer/kafka_producer.py)

# Real-time Processing

Real-time processing is handled by Spark Streaming. Spark consumes the incoming kafka stream and creates RDDs out of it. It then performs natural language processing on the text of each tweet. The topics as well as the sentiment of each tweet are extracted. Both those fields are added as part of each tweets fields. Finally each RDD is written to Elasticsearch. (the code for the producer can be found in consumer/kafka_consumer_clean.py)

# Database

Elasticsearch is queried to display data by the front end. A user's search contains a keyword and a sentiment. Thus the respective elasticsearch query will ask the database to return all tweets that match the topic and sentiment of the query. In order to broaden the result scope, a synonym filter was implemented. With the use of the synonym filter, the results are not limited to the exact search term, but also its synonyms in the filter. (the DB queries can be found in frontend/app/views.py)

(the html code for the front end can be found in frontend/app/templates/)
