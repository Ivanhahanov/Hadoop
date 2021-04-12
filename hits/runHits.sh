#!/bin/bash
for i in {1,2};
do
  yarn jar "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -D mapreduce.job.name="PageRank Job via Streaming" \
        -files $(pwd)/firstMapper,$(pwd)/firstReducer \
        -input HITS/1_"$i" \
        -output HITS/2_"$i" \
        -mapper $(pwd)/firstMapper \
        -reducer $(pwd)/firstReducer
  yarn jar "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -D mapreduce.job.name="PageRank Job via Streaming" \
        -files $(pwd)/secondMapper,$(pwd)/secondReducer \
        -input HITS/2_"$i" \
        -output HITS/1_$((i+1)) \
        -mapper $(pwd)/secondMapper \
        -reducer $(pwd)/secondReducer
done
hdfs dfs -cat HITS/1_3/part-00000