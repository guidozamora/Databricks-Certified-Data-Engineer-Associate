-- Databricks notebook source
CREATE TABLE employees
-- USING DELTA
  (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

INSERT INTO employees
VALUES  
  (1, 'Adam', 3500.0),
  (2, 'Sarah', 4020.2),
  (3, 'John', 299.3),
  (4, 'Thomas', 4000.3),
  (5, 'Anna', 2500.0),
  (6, 'Kim', 6200.3)

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

UPDATE employees
SET salary = salary + 100
WHERE name LIKE 'A%'

-- COMMAND ----------

DESCRIBE HISTORY employees
