# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/Odinschool/raw/circuits.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from circuits_csv

# COMMAND ----------

# Create a view or table

temp_table_name = "circuits_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `circuits_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "circuits_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("cp")

# COMMAND ----------

dbutils.data.help()

# COMMAND ----------

# MAGIC %md
# MAGIC Load Data Frame
# MAGIC
# MAGIC

# COMMAND ----------

data = [(1, 'Dipak'),(2,'Sagar'),(3,'Sudhir'),(4,'Pravin')]
col = ['id','Name']

df = spark.createDataFrame(data,col)
display(df)

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

dbutils.fs.cp('/FileStore/tables/employees.csv','/FileStore/employees.csv')

# COMMAND ----------

dbutils.fs.head('/FileStore/employees.csv')

# COMMAND ----------

dbutils.fs.head('/FileStore/employees.csv',2)

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/data')

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/data1')

# COMMAND ----------

dbutils.fs.rm('/FileStore/data1')

# COMMAND ----------

WIDGET

-Create widget Text
-Create Dropdown
-Remove widget

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text First_Name default 'Dipak'

# COMMAND ----------

# MAGIC %sql
# MAGIC create WIDGET dropdown Gender default 'Male' choices select 'Male'

# COMMAND ----------

# MAGIC %md
# MAGIC Create Data Frame

# COMMAND ----------

data = [(1,'Dipak','Male'),(2,'Sudhir','Male'),(3,'Sagar','Male'),(4,'Sonali','Female')]
cols = ['Id','Name','Gender']
df = spark.createDataFrame(data,cols)
display(df)

# COMMAND ----------

df.createOrReplaceTempView('voters')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM voters

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget dropdown GenderD default'Female' choices select distinct Gender from voters

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from voters where gender = getargument('GenderD')

# COMMAND ----------

# MAGIC %sql
# MAGIC remove widget Gender

# COMMAND ----------


