-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Chapter 1

-- COMMAND ----------

USE CATALOG hive_metastore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Tables

-- COMMAND ----------

CREATE TABLE product_info (
  product_id INT,
  product_name STRING,
  category STRING,
  price DOUBLE,
  quantity INT
)
USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The statement USING DELTA is not necesary because DELTA is the default table format in Databricks
