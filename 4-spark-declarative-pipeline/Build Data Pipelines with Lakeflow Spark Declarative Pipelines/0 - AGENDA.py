# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Build Data Pipelines with Lakeflow Spark Declarative Pipelines (SDP)
# MAGIC
# MAGIC This course introduces users to the essential concepts and skills needed to build data pipelines using Lakeflow Spark Declarative Pipelines (SDP) in Databricks for incremental batch or streaming ingestion and processing through multiple streaming tables and materialized views. Designed for data engineers new to Spark Declarative Pipelines, the course provides a comprehensive overview of core components such as incremental data processing, streaming tables, materialized views, and temporary views, highlighting their specific purposes and differences.
# MAGIC
# MAGIC Topics covered include:
# MAGIC
# MAGIC - Developing and debugging ETL pipelines with the multi-file editor in Spark Declarative Pipelines using SQL (with Python code examples provided)
# MAGIC
# MAGIC - How Spark Declarative Pipelines track data dependencies in a pipeline through the pipeline graph
# MAGIC
# MAGIC - Configuring pipeline compute resources, data assets, trigger modes, and other advanced options
# MAGIC
# MAGIC Next, the course introduces data quality expectations in Spark Declarative Pipelines, guiding users through the process of integrating expectations into pipelines to validate and enforce data integrity. Learners will then explore how to put a pipeline into production, including scheduling options, and enabling pipeline event logging to monitor pipeline performance and health.
# MAGIC
# MAGIC Finally, the course covers how to implement Change Data Capture (CDC) using the AUTO CDC INTO syntax within Spark Declarative Pipelines to manage slowly changing dimensions (SCD Type 1 and Type 2), preparing users to integrate CDC into their own pipelines.
# MAGIC
# MAGIC ---
# MAGIC ### Course Agenda
# MAGIC The following modules are part of the **Data Engineer Learning** Path by Databricks Academy.
# MAGIC | # | Notebook Name |
# MAGIC | --- | --- |
# MAGIC | 1 | [REQUIRED - Course Setup and Creating a Pipeline]($./1 - REQUIRED - Course Setup and Creating a Pipeline) |
# MAGIC | 2 | [Developing a Simple Pipeline]($./2 - Developing a Simple Pipeline) |
# MAGIC | 3 | [Adding Data Quality Expectations]($./3 - Adding Data Quality Expectations) |
# MAGIC | 4L | [Create a Pipeline]($./4 Lab - Create a Pipeline) |
# MAGIC | 5 | [Deploying a Pipeline to Production]($./5 - Deploying a Pipeline to Production) |
# MAGIC | 6 | [Change Data Capture with AUTO CDC with SCD TYPE 1]($./6 - Change Data Capture with AUTO CDC with SCD TYPE 1) |
# MAGIC | 7L | [AUTO CDC INTO with SCD Type 1]($./7 BONUS Lab - AUTO CDC INTO with SCD Type 1) |
# MAGIC
# MAGIC --- 
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use the following Databricks runtime: **`17.3.x-scala2.13`**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
