#!/usr/bin/env python

from flask import Flask, g, Response, url_for
app = Flask(__name__, static_url_path='')

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

import json
import logging

logging.basicConfig()
log = logging.getLogger()
log.setLevel('INFO')

class CassandraClient:
    session = None
    fetch_word_results_statement = None
    fetch_page_details_statement = None

    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        self.session.row_factory = dict_factory
        log.info('Connected to cluster: ' + metadata.cluster_name)
        
        self.fetch_word_results_statement = self.session.prepare("""
                SELECT url, relevance FROM wikipedia.inverted
                WHERE keyword = ? ORDER BY relevance DESC LIMIT 5 ;
           """)
        self.fetch_page_details_statement = self.session.prepare("""
                SELECT * FROM wikipedia.pages
                WHERE url = ? LIMIT 1;
           """)
        
    def fetchWordResults(self, keyword):
        return self.session.execute(self.fetch_word_results_statement.bind([keyword]))

    def fetchPageDetails(self, url):
        return self.session.execute(self.fetch_page_details_statement.bind([url]))

    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()
        log.info('Connection closed.')

db = None
def get_cassandra():
    global db
    if db is None:
        db = CassandraClient()
        db.connect(['127.0.0.1'])
    return db

@app.route('/')
@app.route('/index.html')
def search():
    return app.send_static_file('search.html')

@app.route('/narrow.css')
def send_css():
    return app.send_static_file('narrow.css')

@app.route('/search.js')
def send_js():
    return app.send_static_file('search.js')

@app.route('/favicon.ico')
def send_icon():
    return app.send_static_file('favicon.ico')

@app.route('/word/<keyword>')
def fetch_word(keyword):
    db = get_cassandra()
    
    pages = []
    results = db.fetchWordResults(keyword)
    for hit in results:
        pages.append(db.fetchPageDetails(hit["url"]))

    return Response(json.dumps(pages), status=200, mimetype="application/json")

if __name__ == '__main__':
    app.run()
