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
-- MAGIC # Lab - Creating Bronze Tables from JSON Files
-- MAGIC ### Duration: ~ 15 minutes
-- MAGIC
-- MAGIC In this lab you will ingest a JSON file as Delta table and then flatten the JSON formatted string column.
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC   - Inspect a raw JSON file.
-- MAGIC   - Read in JSON files to a Delta table and flatten the JSON formatted string column.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Setup
-- MAGIC Download **2-lakeflow-data-ingestion/07L-json-data**
-- MAGIC to local computer

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup $reset_all_data=false

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS demo_07L_json_raw_data; --create volume directory

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### upload files **2-lakeflow-data-ingestion/07L-json-data** to demo_07L_json_raw_data

-- COMMAND ----------

-- DBTITLE 1,checking files
LIST '/Volumes/main/dbdemos_data_ingestion/demo_07l_json_raw_data/07L-json-data/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Lab - JSON Ingestion
-- MAGIC **Scenario:** You are working with your data team on ingesting a JSON file into Databricks. Your job is to ingest the JSON file as is into a bronze table, then create a second bronze table that flattens the JSON formatted string column in the raw bronze table for downstream processing.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Inspect the Dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'''
-- MAGIC           SELECT * 
-- MAGIC           FROM json.`/Volumes/main/dbdemos_data_ingestion/demo_07l_json_raw_data/07L-json-data/lab_kafka_events.json`
-- MAGIC           ''').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Create the Raw Bronze Table
-- MAGIC
-- MAGIC Inspect and run the code below to ingest the raw JSON file `/Volumes/workspace/default/demo_07l_json_raw_data/07L-json-data/lab_kafka_events.json` and create the **lab7_lab_kafka_events_raw** table.
-- MAGIC
-- MAGIC Notice the following:
-- MAGIC - The **value** column is decoded.
-- MAGIC - The **decoded_value** column was created and returns the decoded column as a JSON-formatted string.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE lab7_lab_kafka_events_raw
AS
SELECT 
  *,
  cast(unbase64(value) as STRING) as decoded_value
FROM read_files(
        '/Volumes/main/dbdemos_data_ingestion/demo_07l_json_raw_data/07L-json-data/lab_kafka_events.json',
        format => "json", 
        schema => '''
          key STRING, 
          timestamp DOUBLE, 
          value STRING
        ''',
        rescueddatacolumn => '_rescued_data'
      );

-- View the table
SELECT *
FROM lab7_lab_kafka_events_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B3. Create the Flattened Bronze Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Your goal is to flatten the JSON formatted string column **decoded_value** from the table **lab7_lab_kafka_events_raw** to create a new table named **lab7_lab_kafka_events_flattened** for downstream processing. The table should contain the following columns:
-- MAGIC     - **key**
-- MAGIC     - **timestamp**
-- MAGIC     - **user_id**
-- MAGIC     - **event_type**
-- MAGIC     - **event_timestamp**
-- MAGIC     - **items**
-- MAGIC
-- MAGIC     You can use whichever technique you prefer:
-- MAGIC
-- MAGIC     - Parse the JSON formatted string (easiest) to flatten
-- MAGIC       - [Query JSON strings](https://docs.databricks.com/aws/en/semi-structured/json):
-- MAGIC
-- MAGIC     - Convert the JSON formatted string as a VARIANT and flatten
-- MAGIC       - [parse_json function](https://docs.databricks.com/gcp/en/sql/language-manual/functions/parse_json)
-- MAGIC
-- MAGIC     - Convert the JSON formatted string to a STRUCT and flatten
-- MAGIC       - [schema_of_json function](https://docs.databricks.com/aws/en/sql/language-manual/functions/schema_of_json)
-- MAGIC       - [from_json function](https://docs.databricks.com/gcp/en/sql/language-manual/functions/from_json)
-- MAGIC
-- MAGIC **NOTE:** View the lab solution notebook to view the solutions for each.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. To begin, run the code below to view the final solution table **lab7_lab_kafka_events_flattened_solution**. This will give you an idea of what your final table should look like.
-- MAGIC
-- MAGIC   **NOTE**: Depending on your solution, the data types of the columns may vary slightly.  
-- MAGIC
-- MAGIC
-- MAGIC ##### Optional Challenge
-- MAGIC
-- MAGIC   As a challenge, after flattening the table, try converting the data types accordingly. Depending on your skill set, you may not convert all columns to the correct data types within the allotted time.
-- MAGIC
-- MAGIC   - **key** STRING
-- MAGIC   - **timestamp** DOUBLE
-- MAGIC   - **user_id** STRING
-- MAGIC   - **event_type** STRING
-- MAGIC   - **event_timestamp** TIMESTAMP
-- MAGIC   - **items** (STRUCT or VARIANT) depending on the method you used.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Write the query in the cell below to read the **lab_kafka_events_raw** table and create the flattened table **lab7_lab_kafka_events_flattened** following the requirements from above.

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC ---- Parse the JSON formatted STRING
-- MAGIC CREATE OR REPLACE TABLE lab7_lab_kafka_events_flattened_str
-- MAGIC AS
-- MAGIC SELECT 
-- MAGIC   key,
-- MAGIC   timestamp,
-- MAGIC   decoded_value:user_id,
-- MAGIC   decoded_value:event_type,
-- MAGIC   cast(decoded_value:event_timestamp AS TIMESTAMP),
-- MAGIC   from_json(decoded_value:items,'ARRAY<STRUCT<item_id: STRING, price_usd: DOUBLE, quantity: BIGINT>>') AS items
-- MAGIC FROM lab7_lab_kafka_events_raw;
-- MAGIC
-- MAGIC
-- MAGIC ---- Display the table
-- MAGIC SELECT *
-- MAGIC FROM lab7_lab_kafka_events_flattened_str;

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC ---- Convert the JSON formatted string as a VARIANT
-- MAGIC ---- NOTE: The VARIANT decoded_value_variant column is included in this solution to display the column
-- MAGIC ---- NOTE: Variant data type will not work on Serverless Version 1.
-- MAGIC CREATE OR REPLACE TABLE lab7_lab_kafka_events_flattened_variant
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   key,
-- MAGIC   timestamp,
-- MAGIC   parse_json(decoded_value) AS decoded_value_variant,
-- MAGIC   cast(decoded_value_variant:user_id AS STRING),
-- MAGIC   decoded_value_variant:event_type :: STRING,
-- MAGIC   decoded_value_variant:event_timestamp :: TIMESTAMP,
-- MAGIC   decoded_value_variant:items
-- MAGIC FROM lab7_lab_kafka_events_raw;
-- MAGIC
-- MAGIC
-- MAGIC ---- Display the table
-- MAGIC SELECT *
-- MAGIC FROM lab7_lab_kafka_events_flattened_variant;

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC ---- Convert the JSON formatted string as a STRUCT
-- MAGIC
-- MAGIC ---- Return the structure of the JSON formatted string
-- MAGIC SELECT schema_of_json(decoded_value)
-- MAGIC FROM lab7_lab_kafka_events_raw
-- MAGIC LIMIT 1;
-- MAGIC
-- MAGIC
-- MAGIC ---- Use the JSON structure from above within the from_json function to convert the JSON formatted string to a STRUCT
-- MAGIC ---- NOTE: The STRUCT decoded_value_struct column is included in this solution to display the column
-- MAGIC CREATE OR REPLACE TABLE lab7_lab_kafka_events_flattened_struct
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   key,
-- MAGIC   timestamp,
-- MAGIC   from_json(decoded_value, 'STRUCT<event_timestamp: STRING, event_type: STRING, items: ARRAY<STRUCT<item_id: STRING, price_usd: DOUBLE, quantity: BIGINT>>, user_id: STRING>') AS decoded_value_struct,
-- MAGIC   decoded_value_struct.user_id,
-- MAGIC   decoded_value_struct.event_type,
-- MAGIC   cast(decoded_value_struct.event_timestamp AS TIMESTAMP),
-- MAGIC   decoded_value_struct.items
-- MAGIC FROM lab7_lab_kafka_events_raw;
-- MAGIC
-- MAGIC
-- MAGIC ---- Display the table
-- MAGIC SELECT *
-- MAGIC FROM lab7_lab_kafka_events_flattened_struct;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
