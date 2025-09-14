
# Databricks notebook source
# MAGIC %md
# MAGIC # SSIS to Databricks DLT Pipeline
# MAGIC 
# MAGIC This Delta Live Tables pipeline implements the bronze-silver-gold architecture
# MAGIC migrated from SSIS packages.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Data Ingestion



@dlt.table(
    name="bronze_[dbo].[customers]",
    comment="Bronze layer for [dbo].[Customers]"
)
def bronze_[dbo].[customers]():
    return (
        spark.readStream
        .format("delta")
        .table("source_db.[dbo].[Customers]")
        .withColumn("_ingestion_time", current_timestamp())
    )



# COMMAND ----------

# MAGIC %md  
# MAGIC ## Silver Layer - Cleaned and Transformed Data



@dlt.table(
    name="silver_3",
    comment="Silver layer for 3",
    table_properties={"delta.autoOptimize.optimizeWrite": "true"}
)
@dlt.expect_all({'ID_not_null': 'ID IS NOT NULL', 'ID_is_integer': 'CAST(ID AS INTEGER) IS NOT NULL', 'LoadDate_is_timestamp': 'CAST(LoadDate AS TIMESTAMP) IS NOT NULL'})
def silver_3():
    return (
        dlt.read('bronze_[dbo]_[customers]')
        .select(ID as ID, Name as Name, CreatedDate as LoadDate)
        .withColumn("_processed_time", current_timestamp())
    )



# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Business-Ready Data



@dlt.table(
    name="gold_3",
    comment="Gold layer fact table"
)
def gold_3():
    return (
        dlt.read("silver_3")
        .withColumn("load_date", current_date())
        .withColumn("last_updated", current_timestamp())
    )



# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality and Monitoring

@dlt.table(
    name="data_quality_metrics",
    comment="Data quality metrics for monitoring"
)
def data_quality_metrics():
    return spark.sql('''
        SELECT 
            table_name,
            COUNT(*) as row_count,
            COUNT(DISTINCT *) as distinct_count,
            current_timestamp() as measurement_time
        FROM (
            
            SELECT '3' as table_name, * FROM LIVE.silver_3
            
            
        )
        GROUP BY table_name
    ''')