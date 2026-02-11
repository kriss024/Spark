import os

os.system("pyspark3 --master yarn \
--name=bik4_ki \
--queue=rim_usr \
--driver-memory=20G \
--num-executors=6 \
--executor-memory=16G \
--executor-cores=4 \
--conf 'spark.sql.adaptive.enabled=true' \
--conf 'spark.dynamicAllocation.enabled=true' \
--conf 'spark.dynamicAllocation.maxExecutors=50' \
--conf 'spark.sql.codegen.wholeStage=true' \
--conf 'spark.sql.inMemoryColumnarStorage.compressed=true' \
")

spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir('hdfs://.../tmp')

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

import datetime
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType

# proste zapytania opisujące bazę
spark.sql("show databases like '*dwh*'").show(20, truncate=False)
spark.sql("show tables in dwh like '*sco*'").show(20, truncate=False)
spark.sql('describe scoring.mapowanie_pkd').show(20, truncate=False)

# przypisanie do zmiennej df taki data_frame wyniku z pseudo sqla
df = spark.sql("select * from schemat.tabela limit 10")
df = spark.table("schemat.tabela").limit(10)
df.show(20, False)

df.printSchema()

df.toPandas().head(20)

# cwiczenia
df = df.filter("repo_date = '2020-11-20'")

df = df.withColumn("when_col", f.when((f.col("State") =='NC') | (f.col("State") =='NE'), 1).otherwise(None))

window = Window.partitionBy("Account").orderBy(f.col("repo_date").desc())

df.withColumn("rn", f.row_number().over(w2))\
  .filter(f.col("rn") == 1).drop("row")

#hive  zapisanie wyniku na hive
df.createOrReplaceTempView("widok") # tworzenie widoku danych, krok potrzebny do zapisania na hive, -> do zoptymalizowania aby zapis był bez tego

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

#table partitioning: "orc", "parquet", or "hive"
df.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(table_name)

df.write.partitionBy("repo_date").mode("overwrite").format("orc").saveAsTable(table_name)

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

spark.stop()
