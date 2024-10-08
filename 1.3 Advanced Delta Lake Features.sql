-- Databricks notebook source
DESCRIBE HISTORY employees;

-- COMMAND ----------

SELECT *
FROM employees VERSION AS OF 1;

-- COMMAND ----------

SELECT *
FROM employees @v1;

-- COMMAND ----------

DELETE FROM employees;

-- COMMAND ----------

SELECT *
FROM employees;

-- COMMAND ----------

DESCRIBE HISTORY employees;

-- COMMAND ----------

SELECT *
FROM employees@v4;

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 2;

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

DESCRIBE HISTORY employees;

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY id;

-- COMMAND ----------

DESCRIBE DETAIL employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that after the OPTIMIZE command the field number of files was reduced from 4 to 1

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note there are 4 parquet files. But we know that after the OPTIMIZE command the number of data files were reduced to 1. Now we will perform a VACUUM to clean the unnecesary files

-- COMMAND ----------

VACUUM employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After the VACUUM command all the files are still there, that is because we need to specify the retention period. By default retention period is set to 7 days
-- MAGIC

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Running the VACUUM command and setting the retention policy to 0 hours is not allowed. There is a parameter that check that the retention time is at least 7 days

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For demostration purposes **ONLY** we are removing the CHECK of retention policy
-- MAGIC
-- MAGIC **DO NOT DO THIS IN PRD ENVIRONMENTS**

-- COMMAND ----------

-- This is for demostration purposes only. We are removing the CHECK of retention policy
SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, all the vacuumed files have been removed.
-- MAGIC
-- MAGIC But now, as the table had been VACUUM, then there is not TIME TRAVEL

-- COMMAND ----------

SELECT * FROM employees@v3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Drop table

-- COMMAND ----------

DROP TABLE employees;

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'
