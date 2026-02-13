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

# hive  zapisanie wyniku na hive
df.createOrReplaceTempView("widok") # tworzenie widoku danych, krok potrzebny do zapisania na hive, -> do zoptymalizowania aby zapis był bez tego

# cwiczenia
df = df.filter("repo_date = '2020-11-20'")


df = df.withColumn("segment", f.when(f.col("income") < 4000, "LOW")
                              .when(f.col("income") < 7000, "MEDIUM")
                              .otherwise("HIGH"))


window = Window.partitionBy("Account").orderBy(f.col("repo_date").desc())

df = df.withColumn("row_num", f.row_number().over(window))\
  .filter(f.col("row_num") == 1).drop("row_num")


cols_list = df.select("Fund Description").distinct().limit(10).rdd.flatMap(lambda x: x).collect()

value = df.select("Fund Description").first()


df.select("age", "weight", "height").summary("count", "min", "25%", "75%", "max").show()

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# table partitioning: "orc", "parquet", or "hive"
df.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(table_name)

df.write.partitionBy("repo_date").mode("overwrite").format("orc").saveAsTable(table_name)

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

spark.stop()