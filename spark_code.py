# Introductory Guide to PySpark: Common Commands and Best Practices

# 1. Setting Up the Spark Session

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

# 2. Imports

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

# 3. Metadata queries

spark.sql("show databases like '*dwh*'").show(20, truncate=False)
spark.sql("show tables in dwh like '*cust*'").show(20, truncate=False)
spark.sql("describe dwh.customer").show(20, truncate=False)

spark.catalog.listDatabases()
spark.catalog.listTables("dwh")

# 4. Loading Data into DataFrame

data = [
    (1, "Alice", 30, "2023-01-15"),
    (2, "Bob", 25, "2023-02-20"),
    (3, "Charlie", 35, "2023-03-10")
]

df = spark.createDataFrame(data, ["user_id", "name", "age", "join_date"])

df.show(20, False)

df.printSchema()

# 5. Basic DataFrame Operations

df = spark.sql("select * from dwh.customer limit 10")
df = spark.table("dwh.customer").limit(10)

df.show(20, False)

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("users")

# Query the temporary view using SQL
sql_result = spark.sql("""
    SELECT name, age
    FROM users
    WHERE age >= 30
    ORDER BY age DESC
""")

sql_result.show()

# Drop the temporary view (optional)
spark.catalog.dropTempView("users")

# Select specific columns
df.select("name", "age")

# Filter rows (equivalent to WHERE in SQL)
df.filter(f.col("age") > 28)
df.filter("repo_date = '2020-11-20'")

# Add a new column (salary based on age)
df_with_salary = df.withColumn("salary", f.col("age") * 1000)

# Add a constant column
df_with_const = df.withColumn("constant", f.lit(3.14))

# Rename a column
df_renamed = df.withColumnRenamed("old", "new")

# Drop a column
df_dropped = df.drop("join_date")

# Sort the DataFrame
df_sorted = df_renamed.orderBy(f.col("age").desc())

# Display results
df_sorted.show(20, False)

df.toPandas().head(20)

# Count the number of rows
row_count = df_sorted.count()
print(f"Number of rows: {row_count}")

# Fill nulls with a value
df = df.fillna({"income": 0, "category": "UNKNOWN"})

# Join
df_joined = df1.join(df2, on="user_id", how="inner")

# Null handling
df = df.fillna({"amount": 0, "name": "unknown"})
df = df.dropna(subset=["user_id", "amount"])
df.filter(f.col("amount").isNull()).show()
df.filter(f.col("amount").isNotNull()).show()

# String functions
df = df.withColumn("name", f.trim(f.upper(f.col("name"))))
df = df.withColumn("flag", f.col("code").like("PL%"))
df = df.withColumn("flag", f.col("desc").contains("aktywny"))
df = df.withColumn("extracted", f.regexp_extract(f.col("text"), r"(\d+)", 1))
df = df.withColumn("cleaned", f.regexp_replace(f.col("text"), r"\s+", " "))

# Date operations
df = df.withColumn("join_date", f.to_date(f.col("join_date"), "yyyy-MM-dd"))
df = df.withColumn("year", f.year(f.col("join_date")))
df = df.withColumn("month", f.month(f.col("join_date")))
df = df.withColumn("days_since", f.datediff(f.current_date(), f.col("join_date")))
df = df.withColumn("next_month", f.add_months(f.col("join_date"), 1))

# Window functions
window = Window.partitionBy("Account").orderBy(f.col("repo_date").desc())

df = df.withColumn("row_num", f.row_number().over(window))\
    .filter(f.col("row_num") == 1).drop("row_num")

# Group by a column and compute aggregations
df = df.groupBy("category")\
    .agg(
        f.count("*").alias("total_count"),
        f.countDistinct("product").alias("distinct_amounts"),
        f.min("amount").alias("min_amount"),
        f.max("amount").alias("max_amount"),
        f.mean("amount").alias("mean_amount")
    )\
    .orderBy(f.col("category").asc())

# Computes statistics
df.select("age", "weight", "height").summary("count", "min", "25%", "75%", "max").show(20, False)

# Conditions
df = df.withColumn("segment", f.when(f.col("income") < 4000, "LOW")
                              .when(f.col("income") < 7000, "MEDIUM")
                              .otherwise("HIGH"))


sql_case_when = """
CASE
    WHEN salary > 50000 THEN 'High Earner'
    WHEN salary < 50000 THEN 'Low Earner'
    ELSE 'Average Earner'
END
"""

df = df.withColumn("salary_status", f.expr(sql_case_when))

# Union
from functools import reduce
from pyspark.sql import DataFrame

list_of_dfs = [df1, df2, df3, df4]
df = reduce(DataFrame.union, list_of_dfs)
df = reduce(DataFrame.unionByName, list_of_dfs)

# Repartition
df.repartition(200)
df.coalesce(10)

# Cache/persist — essential for iterative workloads
df.cache()
df.persist()
df.unpersist()

df = df.checkpoint()

# 6. Collecting values

value = df.select("Fund Description").first()[0]
cols_list = df.select("Fund Description").distinct().limit(10).rdd.flatMap(lambda x: x).collect()

# 7. Writing data

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# File format: "orc", "parquet", or "hive"
df.write.partitionBy("repo_date").mode("overwrite").format("orc").saveAsTable(table_name)

df.coalesce(1).write.format("parquet").mode("overwrite").save("my_data.parquet")

df.coalesce(1).write.mode("overwrite").option("header", True).option('sep',',').csv("my_data.csv")

# 8. Stop the Spark session

spark.stop()
