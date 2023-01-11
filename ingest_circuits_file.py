# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest circuits.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(), False),
                                     StructField("circuitRef",StringType(), True),
                                     StructField("name",StringType(), True),
                                     StructField("location",StringType(),True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
                                    ])

# COMMAND ----------

circuits_df = spark.read \
            .option("header", True) \
            .schema(circuits_schema) \
            .csv("/mnt/group5dxc/bronze/circuits.csv")

# COMMAND ----------

dbutils.fs.ls("/mnt/group5dxc/bronze/circuits.cs

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# COMMAND ----------

circuits_renamed_df.write.csv("/mnt/group5dxc/silver/circuits")
