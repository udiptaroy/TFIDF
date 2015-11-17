				README

#Used the dsba hadoop cluster for the Assignment(dsba-hadoop.uncc.edu)
ssh -X dsba-hadoop.uncc.edu -l uroy
cd /users/uroy
#Copy the file DocWordCount.java, TermFrequency.java and TFIDF.java to /users/uroy folder
hadoop fs -mkdir /user/uroy
hadoop fs -chown uroy /user/uroy
hadoop fs -mkdir /user/uroy/wordcount /user/uroy/wordcount/input

#Copy all the input test files to /user/uroy/wordcount/input folder
hadoop fs -put file* /user/cloudera/wordcount/input 
mkdir wordcount_classes

#Running DocWordCount.java 
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* -d wordcount_classes DocWordCount.java
jar -cvf docwordcount.jar -C wordcount_classes/ .
hadoop jar docwordcount.jar org.myorg.DocWordCount /user/uroy/wordcount/input /user/uroy/wordcount/output
hadoop fs -cat /user/uroy/wordcount/output/*

#Running TermFrequency.java
hadoop fs -rm -r /user/uroy/wordcount/output 
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* -d wordcount_classes TermFrequency.java
jar -cvf termfrequency.jar -C wordcount_classes/ .
hadoop jar termfrequency.jar org.myorg.TermFrequency /user/uroy/wordcount/input /user/uroy/wordcount/output
hadoop fs -cat /user/uroy/wordcount/output/*

#Running DocWordCount.java
hadoop fs -rm -r /user/uroy/wordcount/output
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* -d wordcount_classes TFIDF.java
jar -cvf tfidf.jar -C wordcount_classes/ .
hadoop jar tfidf.jar org.myorg.TFIDF /user/uroy/wordcount/input /user/uroy/wordcount/output /user/uroy/wordcount/output_2
hadoop fs -cat /user/uroy/wordcount/output_2/*
