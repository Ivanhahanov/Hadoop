#!/bin/bash
hdfs dfs -rm -r HITS/
hdfs dfs -mkdir HITS/
hdfs dfs -mkdir HITS/itr1_1/

hdfs dfs -put input HITS/itr1_1