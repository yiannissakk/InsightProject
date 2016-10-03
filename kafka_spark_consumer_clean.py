import os, math, re, sys, fnmatch, string, json, pandas
import elasticsearch
import nltk
import ast
from nltk import tokenize
from nltk.corpus import stopwords
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from vaderSentiment1 import make_lex_dict, get_sentiment
from pyspark_elastic import EsSparkContext

#1. Topicalizer, taken from (https://gist.github.com/alexbowe/879414) with minor changes

sentence_re = r"""(?x)      # set flag to allow verbose regexps
      ([A-Z])(\.[A-Z])+\.?  # abbreviations, e.g. U.S.A.
    | \w+(-\w+)*            # words with optional internal hyphens
    | \$?\d+(\.\d+)?%?      # currency and percentages, e.g. $12.40, 82%
    | \.\.\.                # ellipsis
    | [][.,;"'?():-_`]      # these are separate tokens
"""

grammar = r"""
    NBAR:
        {<NN.*|JJ>*<NN.*>}  # Nouns and Adjectives, terminated with Nouns

    NP:
        {<NBAR>}
        {<NBAR><IN><NBAR>}  # Above, connected with in/of/etc...
"""

chunker = nltk.RegexpParser(grammar)

lemmatizer = nltk.WordNetLemmatizer()
stemmer = nltk.stem.porter.PorterStemmer()
stopwords = stopwords.words()
printable = set(string.printable)

#returns list of topics from text
def get_topics(text):

        toks = nltk.word_tokenize(text)
        postoks = nltk.tag.pos_tag(toks)
        tree = chunker.parse(postoks)
        terms = get_terms(tree)

        termlist =[]
        for term in terms:
            for word in term:
                termlist.append(word.encode("utf8"))
        return str(termlist)

def leaves(tree):
    """Finds NP (nounphrase) leaf nodes of a chunk tree."""
    for subtree in tree.subtrees(filter = lambda t:  t.label()=='NP'):
        yield subtree.leaves()

def normalise(word):
    """Normalises words to lowercase and stems and lemmatizes it."""
    word = word.lower()
    word = stemmer.stem_word(word)
    word = lemmatizer.lemmatize(word)
    return word

def acceptable_word(word):
    """Checks conditions for acceptable word: length, stopword."""
    accepted = bool(2 <= len(word) <= 40
        and word.lower() not in stopwords)
    return accepted


def get_terms(tree):
    for leaf in leaves(tree):
        term = [ normalise(w) for w,t in leaf if acceptable_word(w) ]
        yield term
      
#1. Topicalizer Code ends here      

#debugging functions
def prnt(x):
    print x

def prnt_tp(x):
    print type(x)

#row rdds: from string to json format 
def try_evaluate(x):
    try:
        jsn_entry = ast.literal_eval(x)
    except:
        jsn_entry = {'source': 0, 'user_id': 0, 'text':"0", 'created_at': "0", 'tweet_id': 0, 'username': "0", 'topics': [], 'sentiment': 0}
    return jsn_entry


def stream_to_dataframe(row_rdd):

        if row_rdd is None or row_rdd.isEmpty():
                print "---------rdd empty!---------"
                return

        else:
                #print "---------rdd not empty!------------"

                #row_rdd.foreach(lambda x: prnt(x))

                #row_rdd.foreach(lambda x: prnt_tp(x))
                #map each row rdd to a elasticsearch writable format  
                new_rdd = row_rdd.map(lambda x: try_evaluate(x))

                #new_rdd.foreach(lambda x: prnt(x))

                #new_rdd.foreach(lambda x: prnt_tp(x))

                #print '________ready to write__________'

                #write to ElasticSearch  
                new_rdd.saveToEs("tweets/docum")

                #print "_____________WROTE TO ES___________________"


if __name__ == '__main__':

        # initialize Spark and set configurations
        conf = SparkConf()
        conf.setAppName("tweet_data")
        conf.setMaster("spark://ec2-52-45-73-216.compute-1.amazonaws.com:7077")
        conf.set("es.nodes", "ec2-52-45-73-216.compute-1.amazonaws.com:9200")
        conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
        sc = EsSparkContext(conf=conf)
        
        #Add files to be downloaded with this Spark job on every node.
        sc.addFile("vaderSentiment1.py")
        sc.addFile("vader_sentiment_lexicon.txt")
            
        ssc = StreamingContext(sc, 2)
        
        #create kafka stream
        kafka_stream = KafkaUtils.createDirectStream(ssc, ["tweets1"], {"metadata.broker.list":"ec2-52-45-73-216.compute-1.amazonaws.com:9092"})

        #kafka stream to RDD
        lines = kafka_stream.map(lambda (y,z): z.split(";"))
        rDD = lines.map(lambda x: "{'source': "+x[0]+", 'user_id': "+x[1]+", 'text': "+"'%s'"%x[2]+", 'created_at': "+"'%s'"%x[3]+", 'tweet_id': "+x[4]+", 'username': "+"'%s'"%x[5]+", 'topics': " + get_topics(x[2])+", 'sentiment': "+get_sentiment(x[2])+"}")
        rDD.foreachRDD(stream_to_dataframe)

        ssc.start()
        ssc.awaitTermination()
