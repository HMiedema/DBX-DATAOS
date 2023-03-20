# Databricks notebook source
# MAGIC %sql
# MAGIC -- Drop tables from previous demo's
# MAGIC DROP TABLE amidbronze.bronze_demo;
# MAGIC DROP TABLE amidbronze.silver_demo;

# COMMAND ----------

# import helper functions from the current repository
import helpers.columns_helpers as ch
# import helpers.data_os as dos
from helpers.data_os import Data_OS

# COMMAND ----------

# Create the Data Operating System Object
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
demo_data_dataframe = spark.createDataFrame(demo_data)
dos.write_dataframe_to_bronze(demo_data_dataframe, bronzeTableName)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if the table was created and __ROW_HASH and __LOADED_AT columns were added
# MAGIC select * from amidbronze.bronze_demo

# COMMAND ----------

# Write the data to silver
# The data has a clear key: name
dos.historize_bronze_table_to_silver(bronzeTableName, silverTableName, "full load with key", keys)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if the table was created and all rows were added
# MAGIC -- Every silver table will get a __START_AT and __END_AT column to show the validity of the record
# MAGIC -- The __CHANGE_TYPE shows the action for the record. In this case it will be all insert
# MAGIC select * from amidbronze.silver_demo

# COMMAND ----------

# Write the same demo data to bronze
demo_data_dataframe = spark.createDataFrame(demo_data)
dos.write_dataframe_to_bronze(demo_data_dataframe, bronzeTableName)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The exact same records were added to the bronze table with only a different __LOADED_AT value
# MAGIC select * from amidbronze.bronze_demo

# COMMAND ----------

# Write the data to silver again
# nothing should happen as there are no changes
dos.historize_bronze_table_to_silver(bronzeTableName, silverTableName, "full load with key", keys)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Indeed the 4 exact same rows are there
# MAGIC select * from amidbronze.silver_demo

# COMMAND ----------

# A new data set with 1 deleted record (name = Bert Lance) and on added record (name = Nico)
demo_data = [{"name": 'Nelson Mandela', "quote": "The greatest glory in living lies not in never falling, but in rising every time we fall."},
        {"name": 'Albert Einstein', "quote": "Insanity is doing the same thing over and over again and expecting different results."},
        {"name": 'John F. Kennedy', "quote": "Those who dare to fail miserably can achieve greatly."},
        {"name": 'Nico', "quote": "Keep it."}
        ]
# Write the same demo data to bronze
demo_data_dataframe = spark.createDataFrame(demo_data)
dos.write_dataframe_to_bronze(demo_data_dataframe, bronzeTableName)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Indeed Nico is in there for the latest __LOADED_AT and Bert Lance is not
# MAGIC select * from amidbronze.bronze_demo

# COMMAND ----------

# Write the data to silver again
# We should see 1 new insert (Nico) and 1 delete (Bert Lance)
dos.historize_bronze_table_to_silver(bronzeTableName, silverTableName, "full load with key", keys)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Indeed the 1 row is updated: name = Bert Lance. __END_AT is set
# MAGIC -- 2 rows are added: 1 with __CHANGE_TYPE = DELETE for Bert Lance. The other is an INSERT for Nico
# MAGIC select * from amidbronze.silver_demo

# COMMAND ----------

# A record is update. Nico's quote is changed
demo_data = [{"name": 'Nelson Mandela', "quote": "The greatest glory in living lies not in never falling, but in rising every time we fall."},
        {"name": 'Albert Einstein', "quote": "Insanity is doing the same thing over and over again and expecting different results."},
        {"name": 'John F. Kennedy', "quote": "Those who dare to fail miserably can achieve greatly."},
        {"name": 'Nico', "quote": "Keep it simple stupid."}
        ]
# Write the same demo data to bronze
demo_data_dataframe = spark.createDataFrame(demo_data)
dos.write_dataframe_to_bronze(demo_data_dataframe, bronzeTableName)
# Historize it to silver again
dos.historize_bronze_table_to_silver(bronzeTableName, silverTableName, "full load with key", keys)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Indeed the old record has been closed. A  new row is open
# MAGIC select * from amidbronze.silver_demo
