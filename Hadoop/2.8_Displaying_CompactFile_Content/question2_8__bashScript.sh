#!/bin/bash

##create folders
hdfs dfs -mkdir -p /user/thib057
hdfs dfs -mkdir /user/thib057/question2.8

##put on HDFS
hdfs dfs -put ./data/noaa.txt /user/thib057/question2.8

## execute JAR job (result displayed in terminal and written in an output file)
hadoop jar 2.8_CompactFileDisplayer.jar CompactFileReader /user/thib057/question2.8/noaa.txt /user/thib057/question2.8/output.txt

##copy ouput to local
hdfs dfs -copyToLocal /user/thib057/question2.8/output.txt ./result/
