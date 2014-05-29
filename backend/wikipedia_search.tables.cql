CREATE KEYSPACE wikipedia WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE wikipedia.pages (
   url text,
   title text,
   abstract text,
   length int,
   refs int,
   PRIMARY KEY (url) 
);

CREATE TABLE wikipedia.inverted (
   keyword text,
   relevance int,
   url text,
   PRIMARY KEY ((keyword), relevance) 
);

