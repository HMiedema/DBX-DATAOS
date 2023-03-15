# Databricks notebook source
# import helper functions from the current repository
import helpers.columns_helpers as ch
import helpers.data_os as dos

# COMMAND ----------

demo_data = [{"name": 'Nelson Mandela', "quote": "The greatest glory in living lies not in never falling, but in rising every time we fall."},
        {"name": 'Albert Einstein', "quote": "Insanity is doing the same thing over and over again and expecting different results."},
        {"name": 'John F. Kennedy', "quote": "Those who dare to fail miserably can achieve greatly."},
        {"name": 'Bert Lance', "quote": "If it ain’t broke, don’t fix it."}
        ]

bronzeTableName = "amidbronze.raw_test"
silverTableName = "amidbronze.silver_test"
keys = ["name"]

# COMMAND ----------

dos.historize_bronze_table_to_silver(bronzeTableName, "amidbronze.silver_test_duplicates", "full load without key with possible duplicate rows", keys)
