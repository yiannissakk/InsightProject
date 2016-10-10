from app import app, es
import uuid
from datetime import datetime
from flask import render_template, request, jsonify

@app.route('/ec2-52-54-138-46.compute-1.amazonaws.com')
@app.route('/index')
def index():
  return render_template("index.html")

def search_for(term, sentiment):
    print sentiment
    if sentiment == "positive":
        res = es.search(index="tweets",
                    body={
                    "from":0,"size":100,
                    "query": {
                        "constant_score": {
                               "filter":{
                                   "bool": {
                                       "must" : [
                                           {"term": {"sentiment": 1}},
                                           {"term": {"text": term}}]}}}}})
    elif sentiment == "negative":
        res = es.search(index="tweets",
                    body={
                    "from":0,"size":100,
                    "query": {
                        "constant_score":{
                               "filter":{
                                   "bool": {
                                       "must" : [
                                           {"term": {"sentiment": -1}},
                                           {"term": {"text": term}}]}}}}})
    elif sentiment == "no":
        res = es.search(index="tweets",
                    body={
                        "from":0,"size":100,
                        "query": {
                        "constant_score": {
                               "filter":{
                                   "term": {"text": term}}}}})
    return res


@app.route('/search/', methods=['POST'])
def search():
    print 'Got it %s' % request.form.values()
    term = request.form['term']
    sentiment = request.form['sentiment']
    print 'Searching for term '+term+' with '+sentiment+ ' sentiment'
    results = search_for(term, sentiment)
    print results
    return render_template('search.html', results=results['hits'])

