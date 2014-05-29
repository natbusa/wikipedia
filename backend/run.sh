#!/bin/sh

$HADOOP_HOME/bin/hadoop fs -ls wikipedia-latest-abstract
if  [ $? -ne 0 ] 
  then $HADOOP_HOME/bin/hadoop fs -mkdir wikipedia-latest-abstract/
fi

$HADOOP_HOME/bin/hadoop fs -ls wikipedia-latest-abstract-small
if  [ $? -ne 0 ] 
  then $HADOOP_HOME/bin/hadoop fs -mkdir wikipedia-latest-abstract-small/
fi

#check if the lorem.txt exists on hdfs
$HADOOP_HOME/bin/hadoop fs -ls wikipedia-latest-abstract-small/enwiki-latest-abstract12.xml
if  [ $? -ne 0 ]
  then $HADOOP_HOME/bin/hadoop fs -copyFromLocal enwiki-latest-abstract12.xml wikipedia-latest-abstract-small/
fi

$HADOOP_HOME/bin/hadoop fs -ls wikipedia-latest-abstract-small/enwiki-latest-abstract.xml
if  [ $? -ne 0 ]
  then $HADOOP_HOME/bin/hadoop fs -copyFromLocal enwiki-latest-abstract.xml wikipedia-latest-abstract/
fi

#set output
HADOOP_OUTPUT_DIR=wikipedia-latest-abstract_to_cassandra

# cleanup output
$HADOOP_HOME/bin/hadoop fs -rm -r -f $HADOOP_OUTPUT_DIR
#execute it

#$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.4.0.jar -files mapper.py  -mapper ./mapper.py -input wikipedia-latest-abstract -output $HADOOP_OUTPUT_DIR

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.4.0.jar \
   -files mapper.py,reducer.py                                                             \
   -mapper ./mapper.py                                                                     \
   -reducer ./reducer.py                                                                   \
   -jobconf stream.num.map.output.key.fields=1                                             \
   -jobconf stream.num.reduce.output.key.fields=1                                          \
   -jobconf mapred.reduce.tasks=16                                                         \
   -input wikipedia-latest-abstract                                                        \
   -output $HADOOP_OUTPUT_DIR

#print the output
$HADOOP_HOME/bin/hadoop fs -cat "$HADOOP_OUTPUT_DIR/*"
