## RUN MapReduce
```
./bin/hadoop jar ./share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar 
    -input ./words.txt \
    -output /crossCorrelationPairs \   
    -mapper "crossCorrelationPairs -task 0 -phase map" \ 
    -reducer "crossCorrelationPairs -task 0 -phase reduce" \
    -io typedbytes \
    -file ./crossCorrelationPairs \
    -numReduceTasks 1 \
```

```
./bin/hadoop jar ./share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar 
    -input ./pairs.txt \
    -output /crossCorrelationStripes \   
    -mapper "crossCorrelationStripes -task 0 -phase map" \ 
    -reducer "crossCorrelationStripes -task 0 -phase reduce" \
    -io typedbytes \
    -file ./crossCorrelationStripes \
    -numReduceTasks 1 \
```