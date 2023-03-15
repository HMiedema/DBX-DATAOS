# Databricks notebook source
# import helper functions from the current repository
import helpers.columns_helpers as ch
# import helpers.data_os as dos
from helpers.data_os import Data_OS

# COMMAND ----------

# Create the Data Operating System
dos = Data_OS(spark)
# dos.historize_bronze_table_to_silver(bronzeTableName, silverTableName, "full load with key", keys)

# COMMAND ----------

demo_data = [{"name": 'Nelson Mandela', "quote": "The greatest glory in living lies not in never falling, but in rising every time we fall."},
        {"name": 'Albert Einstein', "quote": "Insanity is doing the same thing over and over again and expecting different results."},
        {"name": 'John F. Kennedy', "quote": "Those who dare to fail miserably can achieve greatly."},
        {"name": 'Bert Lance', "quote": "If it ain’t broke, don’t fix it."}
        ]

bronzeTableName = "amidbronze.bronze_demo"
silverTableName = "amidbronze.silver_demo"
keys = ["name"]


# COMMAND ----------

# Write the demo data to bronze
# demo_data_dataframe = spark.createDataFrame(demo_data)
# dos.write_dataframe_to_bronze(demo_data_dataframe, bronzeTableName)

# COMMAND ----------

%sql
-- Check if the table was created and __ROW_HASH and __LOADED_AT columns were added
select * from amidbronze.bronze_demo

# COMMAND ----------

# Write the data to silver
# The data has a clear key: name
dos.historize_bronze_table_to_silver(bronzeTableName, silverTableName, "full load with key", keys)

# COMMAND ----------

%sql
-- Check if the table was created and all rows were added
-- Every silver table will get a __START_AT and __END_AT column to show the validity of the record
-- The __CHANGE_TYPE shows the action for the record. In this case it will be all insert
select * from amidbronze.silver_demo

# COMMAND ----------

# Write the same demo data to bronze
demo_data_dataframe = spark.createDataFrame(demo_data)
dos.write_dataframe_to_bronze(demo_data_dataframe, bronzeTableName)

# COMMAND ----------

%sql
-- The exact same records were added to the bronze table with only a different __LOADED_AT value
select * from amidbronze.bronze_demo