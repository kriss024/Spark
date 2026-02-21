from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("PySparkApplication")\
    .master("local[*]")\
    .enableHiveSupport()\
    .config("spark.sql.shuffle.partitions", "20")\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()

print(f"Spark Version: {spark.version}")

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

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
import pandas as pd
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, DecimalType, DoubleType, FloatType, StringType

from pyspark.sql.functions import (
    col, lit, when, sum, count, avg, max, min, 
    concat, upper, lower, trim, date_format, to_date
)

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)

# proste zapytania opisujące bazę
spark.sql("show databases like '*dwh*'").show(20, truncate=False)
spark.sql("show tables in dwh like '*sco*'").show(20, truncate=False)
spark.sql('describe scoring.mapowanie_pkd').show(20, truncate=False)

df.printSchema()

# przypisanie do zmiennej df taki data_frame wyniku z pseudo sqla
df = spark.sql("select * from schemat.tabela limit 10")
df = spark.table("schemat.tabela").limit(10)

df.show(20, False)
df.toPandas().head(20)

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

# cwiczenia
data = [
    (1, "Alice", 30, "2023-01-15"),
    (2, "Bob", 25, "2023-02-20"),
    (3, "Charlie", 35, "2023-03-10")
]

df = spark.createDataFrame(data, ["user_id", "name", "age", "join_date"])


df = df.withColumn("tax", f.col("salary") * 0.1)

df = df.withColumn("const", f.lit(3.14))

df = df.filter("repo_date = '2020-11-20'")

df = df.withColumn("segment", f.when(f.col("income") < 4000, "LOW")
                              .when(f.col("income") < 7000, "MEDIUM")
                              .otherwise("HIGH"))

df = df.groupBy("category")\
    .agg(
        f.count("*").alias("total_count"),
        f.countDistinct("product").alias("distinct_amounts"),
        f.min("amount").alias("min_amount"),
        f.max("amount").alias("max_amount"),
        f.mean("amount").alias("mean_amount")
    )\
    .orderBy(f.col("category").asc())


sql_case_when = """
CASE 
    WHEN salary > 50000 THEN 'High Earner'
    WHEN salary < 50000 THEN 'Low Earner'
    ELSE 'Average Earner'
END
"""

df = df.withColumn("salary_status", f.expr(sql_case_when))

df.createOrReplaceTempView("widok") # tworzenie widoku danych

df = df.checkpoint()

df.select("age", "weight", "height").summary("count", "min", "25%", "75%", "max").show(20, False)

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

window = Window.partitionBy("Account").orderBy(f.col("repo_date").desc())

df = df.withColumn("row_num", f.row_number().over(window))\
  .filter(f.col("row_num") == 1).drop("row_num")

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

value = df.select("Fund Description").first()[0]

cols_list = df.select("Fund Description").distinct().limit(10).rdd.flatMap(lambda x: x).collect()

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

from functools import reduce
from pyspark.sql import DataFrame

list_of_dfs = [df1, df2, df3, df4]
df = reduce(DataFrame.union, list_of_dfs)
df = reduce(DataFrame.unionByName, list_of_dfs)

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# rodzaje partycji na hive: "orc", "parquet", or "hive"
df.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(table_name)

df.write.partitionBy("repo_date").mode("overwrite").format("orc").saveAsTable(table_name)

#~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.~.

spark.stop()
