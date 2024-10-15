-- Databricks notebook source
-- MAGIC %run /Workspace/Users/andrei.zamora@hpe.com/Andrei-Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders;

-- COMMAND ----------

SELECT 
  order_id,
  books,
  FILTER (books, i -> i.quantity >= 2) AS multiple_copies
FROM orders

-- COMMAND ----------

-- Higher Order Function: FILTER
SELECT 
  order_id, 
  multiple_copies
FROM (
  SELECT
    order_id,
    FILTER(books, i -> i.quantity >= 2) AS multiple_copies
  FROM orders)
WHERE size(multiple_copies) > 0;

-- COMMAND ----------

-- Higher Order Function: TRANSFORM
SELECT 
  order_id,
  books,
  TRANSFORM (
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
   ) AS subtotal_after_discount
FROM orders;
  

-- COMMAND ----------

-- User Define Function
CREATE OR REPLACE FUNCTION get_url (email STRING)
RETURNS STRING

RETURN CONCAT("https://www.", split(email, "@")[1]);

-- COMMAND ----------

SELECT email, get_url(email) AS domain
FROM customers;

-- COMMAND ----------

DESCRIBE FUNCTION get_url

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url

-- COMMAND ----------

CREATE OR REPLACE FUNCTION site_type(email STRING)
RETURNs STRING
RETURN CASE
        WHEN email LIKE '%.com' THEN 'Commercial business'
        WHEN email LIKE '%.org' THEN 'Non-profit org'
        WHEN email LIKE '%.edu' THEN 'Educational institution'
        ELSE CONCAT("Unknow extension for domain: ", split(email,'@')[1])
    END;

-- COMMAND ----------

SELECT 
  email,
  site_type(email) AS domain_category
FROM customers;
