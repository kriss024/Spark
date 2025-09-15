import os
path="/home/Work/Spark"
os.chdir(path)

os.system("pyspark3 --master yarn \
--name=SparkApp \
--queue=SparkQueue \
--driver-memory=16G \
--num-executors=6 \
--executor-memory=16G \
--executor-cores=4 \
--conf 'spark.sql.catalogImplementation=hive' \
--conf 'spark.sql.adaptive.enabled=true' \
--conf 'spark.dynamicAllocation.enabled=true' \
--conf 'spark.dynamicAllocation.maxExecutors=50' \
--conf 'spark.sql.codegen.wholeStage=true' \
--conf 'spark.sql.inMemoryColumnarStorage.compressed=true' \
")

spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir('hdfs://.../tmp')
