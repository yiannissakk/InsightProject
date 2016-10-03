from flask import Flask
from flask_elasticsearch import FlaskElasticsearch

app = Flask(__name__)

app.config['SECRET_KEY'] = 'XXXXXXXXXXXXXXXXXX'

app.config['ELASTICSEARCH_HOST'] = 'ec2-52-45-73-216.compute-1.amazonaws.com'

es = FlaskElasticsearch(app)

from app import views

