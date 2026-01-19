-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img
-- MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
-- MAGIC     alt="Databricks Learning"
-- MAGIC   >
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2A - Data Ingestion with CREATE TABLE AS and COPY INTO
-- MAGIC
-- MAGIC In this demonstration, we'll explore ingesting data from cloud storage into Delta tables with the `CREATE TABLE AS (CTAS)` AND `COPY INTO` statements.
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC
-- MAGIC - Use the CTAS statement with `read_files()` to ingest Parquet files into a Delta table.
-- MAGIC - Use `COPY INTO` to incrementally load Parquet files from cloud object storage into a Delta table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this notebook.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup $reset_all_data=true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Explore the Data Source Files
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. We'll create a table containing historical user data from Parquet files stored in the volume  
-- MAGIC    `'/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet'` within Unity Catalog.
-- MAGIC
-- MAGIC    Use the `LIST` statement to view the files in this volume. Run the cell and review the results.
-- MAGIC
-- MAGIC    Notice the files in the **name** column begin with **part-**. This shows that this volume contains multiple **Parquet** files.

-- COMMAND ----------

-- DBTITLE 1,List files in a raw/users-historical volume
LIST '/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query the Parquet [files by path](https://docs.databricks.com/aws/en/query/#query-data-by-path) in the `/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet` directory to view the raw data in tabular format to quickly preview the files.

-- COMMAND ----------

-- DBTITLE 1,View parquet data
SELECT * 
FROM parquet.`/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Batch Data Ingestion with CTAS and read_files()
-- MAGIC
-- MAGIC The `CREATE TABLE AS` (CTAS) statement is used to create and populate tables using the results of a query. This allows you to create a table and load it with data in a single step, streamlining data ingestion workflows.
-- MAGIC
-- MAGIC #### Automatic Schema Inference for Parquet Files
-- MAGIC
-- MAGIC Apache Parquet is a columnar storage format optimized for analytical queries. It includes embedded schema metadata (e.g., column names and data types), which enables automatic schema inference when creating tables from Parquet files. This eliminates the need for manual schema definitions and simplifies the process of converting Parquet files into Delta format by leveraging the built-in schema metadata.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. CTAS with the `read_files()` Function
-- MAGIC
-- MAGIC The code in the next cell creates a table using CTAS with the `read_files()` function.
-- MAGIC
-- MAGIC The `read_files()` table-valued function (TVF) enables reading a variety of file formats and provides additional options for data ingestion.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. First, let's explore the documentation for `read_files`. Complete the following steps:
-- MAGIC
-- MAGIC    a. Navigate to the [read_files](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files) documentation.
-- MAGIC
-- MAGIC    b. Scroll down and find the **Options** section. Take a moment to explore some of the features of `read_files`.
-- MAGIC
-- MAGIC    c. In the **Options** section, notice the variety of options available based on the file type.
-- MAGIC
-- MAGIC    d. Click on **parquet** and scroll through the available options.
-- MAGIC
-- MAGIC **NOTE:** The `read_files` function provides a wide range of capabilities and specific options for each file type. The previous method used to create a table only works if no additional options are required.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 2. Use the `read_files()` function to query the same Parquet files located in `/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet`. The `LIMIT` clause limits the amount of rows during exploration and development.
-- MAGIC
-- MAGIC    - The first parameter in `read_files` is the path to the data.
-- MAGIC
-- MAGIC    - The `format => "parquet"` option specifies the file format.
-- MAGIC
-- MAGIC    The `read_files` function automatically detects the file format and infers a unified schema across all files. It also supports explicit schema definitions and `schemaHints`. For more details on schema inference capabilities, refer to the [Schema inference](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files#schema-inference) documentation.
-- MAGIC
-- MAGIC **NOTE:** A **_rescued_data** column is automatically included by default to capture any data that doesn’t match the inferred schema.

-- COMMAND ----------

-- DBTITLE 1,Read parquet with read_files
SELECT * 
FROM read_files(
  '/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet',
  format => 'parquet'
)
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Next, let's use `read_files()` with a CTAS statement to create the table **user_parquet_bronze_ctas_rf**, then display the table.
-- MAGIC
-- MAGIC    Notice that the Parquet files were ingested to create a table (Delta by default).

-- COMMAND ----------

-- DBTITLE 1,Create table with read_files
-- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS user_parquet_bronze_ctas_rf;


-- Create the Delta table
CREATE TABLE user_parquet_bronze_ctas_rf 
SELECT * 
FROM read_files(
        '/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet',
        format => 'parquet'
      );


-- Preview the Delta table
SELECT * 
FROM user_parquet_bronze_ctas_rf 
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Run the `DESCRIBE TABLE EXTENDED` statement to view column names, data types, and additional table metadata.  
-- MAGIC
-- MAGIC    Review the results and notice the following:
-- MAGIC    
-- MAGIC    - The table was created in your schema within the course catalog **workspace**.
-- MAGIC
-- MAGIC    - The *Type* row indicates that the table is *MANAGED*.
-- MAGIC
-- MAGIC    - The *Location* row shows the managed cloud storage location.
-- MAGIC    
-- MAGIC    - The *Provider* row specifies that the table is a Delta table.
-- MAGIC

-- COMMAND ----------

DESCRIBE TABLE EXTENDED user_parquet_bronze_ctas_rf;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Managed vs External Tables in Databricks
-- MAGIC
-- MAGIC ##### Managed Tables
-- MAGIC - Databricks **manages both the data and metadata**.
-- MAGIC - Data is stored **within Databricks’ managed storage**.
-- MAGIC - **Dropping the table also deletes the data**.
-- MAGIC - Recommended for creating new tables.
-- MAGIC
-- MAGIC ##### External Tables
-- MAGIC - Databricks **only manages the table metadata**.
-- MAGIC - **Dropping the table does not delete the data**.
-- MAGIC - Supports **multiple formats**, including Delta Lake.
-- MAGIC - Ideal for **sharing data across platforms** or using existing external data.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. (BONUS) Python Ingestion
-- MAGIC The code uses Python to ingest the parquet files.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 1. Read the Parquet files from the volume into a Spark DataFrame
-- MAGIC df = (spark
-- MAGIC       .read
-- MAGIC       .format("parquet")
-- MAGIC       .load("/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet")
-- MAGIC     )
-- MAGIC
-- MAGIC
-- MAGIC # 2. Write to the DataFrame to a Delta table (overwrite the table if it exists)
-- MAGIC (df
-- MAGIC  .write
-- MAGIC  .mode("overwrite")
-- MAGIC  .saveAsTable(f"user_parquet_bronze_ctas_rf")
-- MAGIC )
-- MAGIC
-- MAGIC
-- MAGIC ## 3. Read and view the table
-- MAGIC users_bronze_table = spark.table(f"user_parquet_bronze_ctas_rf")
-- MAGIC users_bronze_table.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Incremental Data Ingestion with `COPY INTO`
-- MAGIC `COPY INTO` is a Databricks SQL command that allows you to load data from a file location into a Delta table. This operation is re-triable and idempotent, i.e. files in the source location that have already been loaded are skipped. This command is useful for when you need to load data into an existing Delta table. 
-- MAGIC
-- MAGIC [COPY INTO](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D1. Ingesting Parquet Files with COPY INTO
-- MAGIC
-- MAGIC Using the same set of Parquet files as before, let's use `COPY INTO` to create our Bronze table again.
-- MAGIC
-- MAGIC We will look at two examples:
-- MAGIC
-- MAGIC 1. Example 1: Common Schema Mismatch Error
-- MAGIC
-- MAGIC 1. Example 2: Preemptively Handling Schema Evolution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Example 1: Common Schema Mismatch Error
-- MAGIC
-- MAGIC 1. The cell below creates an empty table named **user_parquet_bronze_ctas_ci** with a defined schema for only the **id** and **creation_date** columns.
-- MAGIC
-- MAGIC    However, the Parquet files in `'/Volumes/dbacademy_ecommerce/v01/raw/users-historical'` contain three columns: 
-- MAGIC     - **id**
-- MAGIC     - **creation_date** 
-- MAGIC     - **firstname**
-- MAGIC     - **lastname**
-- MAGIC     - **email**
-- MAGIC     - **address**
-- MAGIC     - **gender**
-- MAGIC     - **age_group**
-- MAGIC     - **_rescued_data**
-- MAGIC
-- MAGIC    Run the cell below and review the error. You should see the `[COPY_INTO_SCHEMA_MISMATCH_WITH_TARGET_TABLE]` error. This error occurs because there is a schema mismatch: the Parquet files contain 9 columns, but the target table **user_parquet_bronze_ctas_ci** only has 2 columns.
-- MAGIC
-- MAGIC    How can you handle this error?

-- COMMAND ----------

-- DBTITLE 1,COPY INTO schema mismatch
--------------------------------------------
-- This cell returns an error
--------------------------------------------

-- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS user_parquet_bronze_ctas_ci;


-- Create an empty table with the specified table schema (only 2 out of the 9 columns)
CREATE TABLE user_parquet_bronze_ctas_ci (
  id BIGINT,
  creation_date STRING
);


-- Use COPY INTO to populate Delta table
COPY INTO user_parquet_bronze_ctas_ci
  FROM '/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet'
  FILEFORMAT = parquet;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. We can fix this error by adding `COPY_OPTIONS` with the `mergeSchema = 'true'` option. When set to `true`, this option allows the schema to evolve based on the incoming data.
-- MAGIC
-- MAGIC    Run the next cell with the `COPY_OPTIONS` option added. You should notice that the Parquet files were successfully ingested into the table, with a total of 100,000 rows ingested.

-- COMMAND ----------

-- DBTITLE 1,COPY INTO with schema evolution
COPY INTO user_parquet_bronze_ctas_ci
  FROM '/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet'
  FILEFORMAT = parquet
  COPY_OPTIONS ('mergeSchema' = 'true');     -- Merge the schema of each file

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Preview the data in the **user_parquet_bronze_ctas_ci
-- MAGIC ** table.

-- COMMAND ----------

-- DBTITLE 1,View data in the created table
SELECT *
FROM user_parquet_bronze_ctas_ci
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Example 2: Preemptively Handling Schema Evolution
-- MAGIC
-- MAGIC 1. Another way to ingest the same files into a Delta table is to start by creating an empty table named **user_parquet_bronze_ctas_ci_no_schema**.
-- MAGIC
-- MAGIC    Then, add the `COPY_OPTIONS ('mergeSchema' = 'true')` option to enable schema evolution for the table.
-- MAGIC
-- MAGIC    Run the cell and confirm that 100,000 rows were added to the Delta table.

-- COMMAND ----------

-- DBTITLE 1,COPY INTO with schema evolution
-- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS user_parquet_bronze_ctas_ci_no_schema;


-- Create an empty table without the specified schema
CREATE TABLE user_parquet_bronze_ctas_ci_no_schema;


-- Use COPY INTO to populate Delta table
COPY INTO user_parquet_bronze_ctas_ci_no_schema
  FROM '/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet'
  FILEFORMAT = parquet
  COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D2. Idempotency (Incremental Ingestion)
-- MAGIC
-- MAGIC `COPY INTO` tracks the files it has previously ingested. If the command is run again, no additional data is ingested because the files in the source directory haven't changed.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Let's run the `COPY INTO` command again and check if any data is re-ingested into the table.
-- MAGIC
-- MAGIC    Run the cell and view the results. Notice that the values for **num_affected_rows**, **num_inserted_rows**, and **num_skipped_corrupt_files** are all 0 because the data has already been ingested into the Delta table.
-- MAGIC
-- MAGIC **NOTE**: If new files are added to the cloud storage location, `COPY INTO` will only ingest those files. Using `COPY INTO` is a great option if you want to run a job for incremental batch ingestion from cloud storage location without re-reading files that have already been loaded.

-- COMMAND ----------

-- DBTITLE 1,COPY INTO idempotency
COPY INTO user_parquet_bronze_ctas_ci_no_schema
  FROM '/Volumes/main/dbdemos_data_ingestion/raw_data/user_parquet'
  FILEFORMAT = parquet
  COPY_OPTIONS ('mergeSchema' = 'true');
-- to force override idempotency using: COPY_OPTIONS ('force' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
