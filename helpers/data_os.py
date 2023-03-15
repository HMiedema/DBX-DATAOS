from helpers.columns_helpers import *

from pyspark.sql.functions import lit, col, concat_ws, sha2, expr
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

class Data_OS:
  def __init__(self, spark):
    self.spark = spark
    
  def add_loadAt_column(self, df: DataFrame) -> DataFrame:
      """
      Returns a new list of columns without specified columns
      :param df: dataframe
      :return: new dataframe with one additional column
      """
      now = datetime.utcnow()  
      return df.withColumn("__LOADED_AT", lit(now))  
  def write_dataframe_to_bronze(self, df: DataFrame, tableName: str):
      """
      Returns a new list of columns without specified columns
      :param df: dataframe
      :return: new dataframe with one additional column
      """
      df = df.withColumn("__ROW_HASH", sha2(concat_ws("||", *df.columns), 256))
      df = self.add_loadAt_column(df)
      df.write.format("delta")\
      .mode("append")\
      .saveAsTable(tableName)  

  def check_table_exist(self, db_tbl_name):
      table_exist = False
      
      split_db_tbl_name = db_tbl_name.split(".")  
      if len(split_db_tbl_name) != 2:
          raise Exception('Error: "' + db_tbl_name + '" is not a valid Hive table name. It has to consist out of 2 part separated by a .(dot). E.g. schema.table')
          
      db_name = split_db_tbl_name[0]
      table = split_db_tbl_name[1]  
      if self.spark._jsparkSession.catalog().tableExists(db_name, table):
          table_exist = True
      return table_exist  
  
  def check_column_in_table_exist(self, db_tbl_name: str, columnlist: [str]):
      errorList = []
      bronze_df = self.spark.read.table(db_tbl_name)
      
      for column in columnlist:
          if not column in bronze_df.columns:
              errorList.append('Error: "' + column + '" not available in the bronze table "' + db_tbl_name + '".')  
      return errorList  
  def get_table_property(self, tableName: str, key: str):
      table_properties = self.spark.sql("SHOW TBLPROPERTIES " + tableName).toPandas()
      key_value = table_properties[table_properties.key.eq(key)]
      if len(key_value) == 0:
          return None
      
      return key_value.iat[0 ,1]  
  def set_table_property(self, tableName: str, key: str, value: str):
      self.spark.sql('ALTER TABLE ' + tableName + ' SET TBLPROPERTIES(' + key + ' = "' + value + '")')  

  def get_max_loaded_at(self, tableName: str):
      loadedAtLatest = self.get_table_property(tableName, '__LOADED_AT_LATEST')
      
      if loadedAtLatest == None:
          silver_df = self.spark.read.table(tableName)
          loadedAtLatest = silver_df.select("__START_AT").distinct().toPandas().max()[0]  
      return loadedAtLatest  
  def raiseErrorForList(self, errorList: [str]):
      if len(errorList) > 0:
          raise Exception("\n".join(errorList))  
      
  def historize_bronze_table_to_silver(self, bronzeTableName: str, silverTableName: str, bronzeLoadType: str, keys: [str] = []):
      AcceptedBronzeLoadType = ["full load with key", "full load without key but with unique rows", "full load without key with possible duplicate rows"]
      bronzeLoadType = bronzeLoadType.lower().strip()
      errorList = []
      if not (bronzeLoadType in AcceptedBronzeLoadType):
          errorList.append('Error: "' + bronzeLoadType + '" is not an accepted Load Type. Only the following are accepted: "' + ('", "'.join(AcceptedBronzeLoadType)) + '"')  
      if not self.check_table_exist(bronzeTableName):
          errorList.append('Error: bronze table "' + bronzeTableName + '" does not exist.')
      else:
          errorList.extend(self.check_column_in_table_exist(bronzeTableName, ["__ROW_HASH", "__LOADED_AT"]))
          
      if bronzeLoadType in ["full load with key"]:
          if len(keys) == 0:
              errorList.append('Error: No keys are given. "full load with key" load pattern expects at least one key.')
          errorList.extend(self.check_column_in_table_exist(bronzeTableName, keys))
      
      if bronzeLoadType in ["full load without key"]:
          if len(keys) != 0:
              errorList.append('Error: Keys are given: "' + '", "'.join(keys) + '". "full load without key" load pattern expects no keys.')
              
      self.raiseErrorForList(errorList)
      
      if bronzeLoadType == "full load with key":
          self.historize_bronze_table_to_silver_full_load_with_key(bronzeTableName, silverTableName, keys) 
      elif bronzeLoadType == "full load without key but with unique rows":
          self.historize_bronze_table_to_silver_full_load_without_key_and_unique_rows(bronzeTableName, silverTableName)
      # elif bronzeLoadType == "full load without key with possible duplicate rows":
      #     historize_bronze_table_to_silver_full_load_without_key_and_possible_duplicate_rows(bronzeTableName, silverTableName)
  
  # def historize_bronze_table_to_silver_full_load_without_key_and_possible_duplicate_rows(bronzeTableName: str, silverTableName):
        
  def historize_bronze_table_to_silver_full_load_without_key_and_unique_rows(self, bronzeTableName: str, silverTableName: str):
      self.historize_bronze_table_to_silver_full_load_with_key(bronzeTableName, silverTableName, ['__ROW_HASH']) 
      
  def historize_bronze_table_to_silver_full_load_with_key(self, bronzeTableName: str, silverTableName: str, keys: [str] = []):
      bronze_df = self.spark.read.table(bronzeTableName)
      LoadedAtPandasBronze = bronze_df.select('__LOADED_AT').distinct().toPandas()
      print('The bronze table (' + bronzeTableName + ') has the following loads: ')
      print(LoadedAtPandasBronze)  
      if self.check_table_exist(silverTableName):
          print('Taget table exists (' + silverTableName + '). Checking which target table which loads are already in the target.')
          silver_df = self.spark.read.table(silverTableName)
          max_silver_LoadedAt = self.get_max_loaded_at(silverTableName)  
          print(max_silver_LoadedAt)
          LoadedAtPandasBronze = LoadedAtPandasBronze[(LoadedAtPandasBronze['__LOADED_AT'] > max_silver_LoadedAt)]  
      sortedLoadedAtList = sorted(list(LoadedAtPandasBronze["__LOADED_AT"]))  
      print('--------------------------------------------------------------------------------------------------------------------------------------------------------')
      if len(LoadedAtPandasBronze) == 0:
          print('No new batches in bronze (' + bronzeTableName + ') compared to silver (' + silverTableName + ')')
      else:
          print(str(len(LoadedAtPandasBronze)) + ' new batches in bronze (' + bronzeTableName + ') compared to silver (' + silverTableName + ')')
          cnt = 0
      for loadedAt in sortedLoadedAtList:
          cnt = cnt + 1
          print('--------------------------------------------------------------------------------------------------------------------------------------------------------')
          print('Batch ' + str(cnt) + ' with loadedAt: ' + str(loadedAt))
          bronze_df_iteration = bronze_df.filter(col('__LOADED_AT') == loadedAt)\
          .withColumn("__START_AT", col('__LOADED_AT'))\
          .withColumn("__END_AT", lit(None))\
          .withColumn("__END_AT", col("__END_AT").cast("timestamp"))\
          .withColumn("__CHANGE_TYPE", lit("INSERT"))  
          if not(self.check_table_exist(silverTableName)):
              print(silverTableName + " does not exist")
              bronze_df_iteration.drop(col("__LOADED_AT"))\
              .write.format("delta")\
              .mode("append")\
              .saveAsTable(silverTableName)
          else:
              bronze_df_iteration.createOrReplaceTempView("source_iteration")  
              keys_equal_source_target = " AND ".join(["source." + key + " = target." + key for key in keys])
              keys_source_null = " AND ".join(["source." + key + " IS NULL " for key in keys])
              merge_join_keys = ", ".join([key + " AS " + key + "_MERGE_JOIN"  for key in keys])
              merge_keys_equal_source_target = " AND ".join(["source." + key + "_MERGE_JOIN = target." + key for key in keys])
              updates_and_deletes = '\
                  SELECT null join_key, source.*, target.name AS __TARGET_JOIN_KEY \
                  FROM source_iteration AS source \
                  FULL OUTER JOIN ' + silverTableName + ' AS target  \
                  ON ' + keys_equal_source_target + ' AND target.__END_AT is null \
                  WHERE (source.__ROW_HASH <> target.__ROW_HASH) OR ((' + keys_source_null + ') AND target.__END_AT is null AND target.__CHANGE_TYPE <> "DELETE")'  
              self.spark.sql(updates_and_deletes)\
              .withColumn("__CHANGE_TYPE", expr('IF(name IS NULL, "DELETE", "UPDATE")'))\
              .withColumn("__START_AT", expr('IF(name IS NULL, to_timestamp("' + str(loadedAt) + '"), __START_AT)'))\
              .withColumn("name", col("__TARGET_JOIN_KEY"))\
              .drop("__TARGET_JOIN_KEY")\
              .createOrReplaceTempView("update_and_deletes")        
              all_inserts_updates_deletes = '\
                  SELECT ' + merge_join_keys + ', source_iteration.* \
                  FROM source_iteration \
                  UNION ALL \
                  SELECT * FROM update_and_deletes'  
              self.spark.sql(all_inserts_updates_deletes)\
              .createOrReplaceTempView("inserts_update_and_deletes")  
              merge_source_target = '\
                  MERGE INTO ' + silverTableName + ' AS target \
                  USING inserts_update_and_deletes AS source \
                  ON ' + merge_keys_equal_source_target + ' \
                  WHEN MATCHED AND target.__END_AT IS NULL AND target.__CHANGE_TYPE <> "DELETE" AND target.__ROW_HASH <> source.__ROW_HASH THEN UPDATE SET target.__END_AT = source.__START_AT \
                  WHEN NOT MATCHED THEN INSERT * \
                  WHEN NOT MATCHED BY SOURCE AND target.__END_AT IS NULL AND target.__CHANGE_TYPE <> "DELETE" THEN UPDATE SET target.__END_AT = to_timestamp("' + str(loadedAt) + '")'
              print(merge_source_target)
              self.spark.sql(merge_source_target)
  #             self.spark.sql('ALTER TABLE ' + silverTableName + ' SET TBLPROPERTIES(__LOADED_AT_LATEST = "' + str(loadedAt) + '")')
              self.set_table_property(silverTableName, "__LOADED_AT_LATEST", str(loadedAt))