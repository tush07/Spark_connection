# Spark_connection

HiveTrial is the code for running on standalone 
HiveSparkYarn is the code for running on yarn

the table's schema : (key INT, value STRING)

Commands to run the jar :

For HiveTrial-0.0.1-SNAPSHOT-jar-with-dependencies.jar

spark-submit --class sparkHiveTrial.HiveTrial.App --master spark://master:7077 HiveTrial-0.0.1-SNAPSHOT-jar-with-dependencies.jar arg1 arg2 arg3 arg4 

For  HiveSparkYarn-0.0.1-SNAPSHOT-jar-with-dependencies.jar 

spark-submit --class sparkHiveYarn.HiveSparkYarn.App --executor-memory 1g HiveSparkYarn-0.0.1-SNAPSHOT-jar-with-dependencies.jar targ1 arg2 arg3 arg4 


1st argument (arg1) is the metastore uri

2nd argument  (arg2) is the name of the table

3rd argument  (arg3) is the path for the file which has data that has to be loaded

4th argument  (arg4) is the path for the output directory

