-- Databricks notebook source
-- MAGIC %fs ls

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/tables

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val insurance = spark.read.format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .load("dbfs:/FileStore/tables/insurance.csv")
-- MAGIC 
-- MAGIC display(insurance)

-- COMMAND ----------

select sex, count(*) from insurance_csv group by 1

-- COMMAND ----------

--%sql
-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
CREATE TEMPORARY TABLE insurance2
  USING csv
  OPTIONS (path "/FileStore/tables/insurance.csv", header "true", mode "FAILFAST")

-- COMMAND ----------

select sex, count(*) from insurance_csv group by 1
