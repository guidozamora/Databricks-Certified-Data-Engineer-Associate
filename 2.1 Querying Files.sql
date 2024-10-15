-- Databricks notebook source
-- MAGIC %run /Workspace/Users/andrei.zamora@hpe.com/Andrei-Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

-- Query from single file
SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json `

-- COMMAND ----------

-- Query from several files

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

-- Query from a directory of files

SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT COUNT(*) FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT 
  *,
  input_file_name() source_file
FROM json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

SELECT * FROM text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

--Create table from a folder

CREATE TABLE books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimeter = ";"
)
LOCATION "${dataset.bookstore}/books-csv"

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

DESCRIBE EXTENDED books_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC         .table("books_csv")
-- MAGIC         .write
-- MAGIC             .mode("append")
-- MAGIC             .format("csv")
-- MAGIC             .option("header", "true")
-- MAGIC             .option("delimiter", ";")
-- MAGIC             .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

REFRESH TABLE books_csv

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- Create a Delta Table using files

CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;


DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS 
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- This successfully created a table but it is not well parsed. This is because it is a csv file.
-- Reading data from CSV files does not permit to specify options
-- So we need to do it like in the next cell

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = 'true',
  delimiter = ";"
);

CREATE TABLE books AS 
SELECT * FROM books_tmp_vw;

SELECT * FROM books;

-- COMMAND ----------

DESCRIBE EXTENDED books;
