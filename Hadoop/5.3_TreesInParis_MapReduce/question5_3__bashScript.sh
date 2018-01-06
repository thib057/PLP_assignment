#!/bin/bash

##create folders
hdfs dfs -mkdir -p /user/thib057
hdfs dfs -mkdir /user/thib057/data

##put on HDFS
hdfs dfs -put ./data/arbres.csv /user/thib057/data

## execute JAR job1 (number of trees by type), in one output dir
hadoop jar 5.3_ParisTrees_MapReduce.jar Number_By_Type /user/thib057/data/arbres.csv /user/thib057/question5.3/numberByType

##execute second jar job in other output dir
hadoop jar 5.3_ParisTrees_MapReduce.jar MaxHeight_By_Type /user/thib057/data/arbres.csv /user/thib057/question5.3/maxHeightByType

##execute oldest tree borough job
hadoop jar 5.3_ParisTrees_MapReduce.jar OldestTree_Borough /user/thib057/data/arbres.csv /user/thib057/question5.3/oldestTree_borough

##copy ouput to local
hdfs dfs -copyToLocal /user/thib057/question5.3/* ./result
