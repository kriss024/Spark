import os
path="/home/Work/Spark"
os.chdir(path)

os.system("pyspark --name SparkApp --master local[*] --driver-memory 4G --executor-memory 4G --conf spark.sql.catalogImplementation=hive")