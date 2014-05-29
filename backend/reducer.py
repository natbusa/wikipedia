#!/usr/bin/env python

from cassandra.cluster import Cluster
import logging

log = logging.getLogger()
log.setLevel('INFO')

cassandra_client = None

class CassandraClient:
    session = None
    insert_word_statement = None

    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        log.info('Connected to cluster: ' + metadata.cluster_name)
        self.prepareStatements()
        
    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()
        log.info('Connection closed.')

    def prepareStatements(self):
        self.insert_word_statement = self.session.prepare("""
            INSERT INTO wikipedia.inverted
            (keyword, relevance, url)
            VALUES (?, ?, ?);
            """)

    def insertWord(self, keyword, relevance, url):
        self.session.execute(self.insert_word_statement.bind((keyword, relevance, url)))


import sys
from sortedcontainers import SortedDict

current_word = None
sorted_dict  = SortedDict()


def emit_ranking(n=100):
    global sorted_dict
    print '%s (%d)' % (current_word, len(sorted_dict))
    for i in range(n):
        try:
            (relevance, url) = sorted_dict.popitem()
            cassandra_client.insertWord(current_word, relevance, url)
    	except: 
            break

def prune(n=100):
    global sorted_dict
    if len(sorted_dict) > n*10:
        c = 0
        for k in reversed(sorted_dict):
            c += 1
            if c>n:
               sorted_dict.pop(k)   

def readLoop():
    global sorted_dict, current_word
    word = None

    # input comes from STDIN
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()

        # parse the input we got from mapper.py
        word, relevance, url = line.split('\t', 2)

        # convert relevance (currently a string) to int
        relevance = int(relevance)

        # a reducer can handle multiple keys.
        # Must control when a new key set is starting and emit the result
        # before handling the new key set
        if current_word == word :
            sorted_dict[relevance] = url
            prune()
        else:
            if current_word:
                emit_ranking()
        
            #let's do it all over with the new word
            sorted_dict.clear()
            sorted_dict[relevance] = url
            current_word = word

    # do not forget to output the last word if needed!
    if current_word == word and current_word:
        emit_ranking()

def main():
    global cassandra_client

    logging.basicConfig()
    cassandra_client = CassandraClient()
    cassandra_client.connect(['127.0.0.1'])
    readLoop()
    cassandra_client.close()

if __name__ == "__main__":
    main()

