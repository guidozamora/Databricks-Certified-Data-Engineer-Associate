# Databricks notebook source
# MAGIC %run /Workspace/Users/andrei.zamora@hpe.com/Andrei-Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

# COMMAND ----------

(spark.readStream
    .table("books")
    .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vi;
