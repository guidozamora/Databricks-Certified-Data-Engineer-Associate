-- Databricks notebook source
-- Create a managed table
CREATE TABLE managed_default
(width INT,
length INT,
height INT);

INSERT INTO managed_default
VALUES (3 INT,2 INT,1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED managed_default;

-- COMMAND ----------

-- Create an external table
CREATE TABLE external_default
  (width INT,
  length INT,
  height INT)
LOCATION 'dbfs:/mnt/demo/external_default';

INSERT INTO external_default
VALUES(3 INT, 2 INT, 1 INT);



-- COMMAND ----------



-- COMMAND ----------

DESCRIBE EXTENDED external_default;

-- COMMAND ----------

-- Drop managed table
DROP TABLE managed_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

-- Drop external table
DROP TABLE external_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'
-- MAGIC
-- MAGIC -- The data parquet files and the transaction log folder still exists after the DROP command

-- COMMAND ----------

-- Create a new database
CREATE SCHEMA new_default;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default;

-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);

INSERT INTO managed_new_default 
VALUES (3 INT, 2 INT, 1 INT);


----------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_new_default';


INSERT INTO external_new_default 
VALUES (3 INT, 2 INT, 1 INT);


-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default;

-- COMMAND ----------

DESCRIBE EXTENDED external_new_default;

-- COMMAND ----------

DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/users/hive/warehouse/managed_new_default'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_new_default'

-- COMMAND ----------

-- Create a DB in a custom location outside the Hive directory

CREATE DATABASE custom
LOCATION 'dbfs:/Shared/schemas/custom.db'

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED custom;
