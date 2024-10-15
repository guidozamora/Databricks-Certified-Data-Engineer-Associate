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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert Data

-- COMMAND ----------

INSERT INTO product_info 
VALUES (1, 'Winter Jacket', 'Clothing', 79.95, 100);

INSERT INTO product_info 
VALUES 
  (2, 'Microwave', 'Kitchen', 249.75, 30),
  (3, 'Board Game', 'Toys', 29.99, 75),
  (4, 'Smartcar', 'Electronics', 599.99, 50);

