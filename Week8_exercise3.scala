// Databricks notebook source
// MAGIC %fs ls

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/tables

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


    val insurance = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/tables/insurance.csv")
    
   

// COMMAND ----------

    println("Print the size")
    println(insurance.count()+"\n")

    println("Print sex and count of sex (use group by in sql)")
    insurance.groupBy(col1 = "sex").count().show
    println("\n")

    println("Filter smoker=yes and print again the sex,count of sex")
    insurance.select("sex","smoker").where("smoker == 'yes'" ).groupBy(col1 = "sex").count().show
    println("\n")

    println("Group by region and sum the charges (in each region), then print rows by\ndescending order (with respect to sum)")
    insurance.groupBy(col1 = "region").sum("charges").orderBy(desc("sum(charges)")).show
    println("\n")




