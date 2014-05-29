#!/usr/bin/env python
# -*- coding: utf-8 -*-

STOPWORDS = set(["wikipedia", "the", "and", "for"])

from cassandra.cluster import Cluster
import logging

log = logging.getLogger()
log.setLevel('INFO')

cassandra_client = None

#define the cassandra client specific for our wikipedia application
class CassandraClient:
    session = None
    insert_page_statement = None

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
        self.insert_page_statement = self.session.prepare("""
            INSERT INTO wikipedia.pages
            (url, title, abstract, length, refs)
            VALUES (?, ?, ?, ?, ?);
            """)

    def insertPage(self, url, title, abstract, length, refs):
        self.session.execute(self.insert_page_statement.bind((url, title, abstract, length, refs)))



from xml.etree import cElementTree as ET

def processXML(doc):
    try:
        doc = ET.fromstring(doc)

        url = doc.find('url').text
        title = doc.find('title').text
        abstract = doc.find('abstract').text
        links = doc.find('links').findall('sublink')

        #extract words from title
        txt_title = [c for c in title.lower() if c.isalpha() or c == ' ']
        txt_abstract = [c for c in abstract.lower() if c.isalpha() or c == ' ']
        txt = ''.join(txt_title) + ' ' + ''.join(txt_abstract)

        #extract words from title and abstract
        words = [w for w in txt.split() if w not in STOPWORDS and len(w) > 2]

        #char ham vs spam
        ham = len(txt_abstract) > (0.95 * len(abstract))

        #relevance algorithm
        length = len(abstract)
        refs = len(links)
        relevance = length * refs

        #rough filter to remove less relevant entries
        if length > 600 or length < 200 or refs < 10 or 'list' in words or not ham:
            return

        #mapper output to cassandra wikipedia.pages table
        cassandra_client.insertPage(url, title, abstract, length, refs)

        #emit unique the key-value pairs
        emitted = list()
        for word in words:
            if word not in emitted:
                print '%s\t%06d\t%s' % (word, relevance, url)
                emitted.append(word)

    except:
        pass


# input comes from STDIN (standard input)
def readLoop():
    for line in sys.stdin:
        if line.find('<doc>') == 0:
            elem = line
            for line in sys.stdin:
                elem += line
                if line.find('</doc>') == 0:
                    processXML(elem)
                    break


import sys


def main():
    global cassandra_client

    logging.basicConfig()
    cassandra_client = CassandraClient()
    cassandra_client.connect(['127.0.0.1'])
    readLoop()
    cassandra_client.close()


if __name__ == "__main__":
    main()

