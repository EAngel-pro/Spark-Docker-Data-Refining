#init
from pyspark.sql import SparkSession
from pathlib import Path
from datetime import datetime
import json
import http.client
import requests
import sys
import pandas
import os
import shutil
spark=SparkSession.builder.appName('Dataframe').getOrCreate()
sqlContext = SparkSession(spark)
spark.sparkContext.setLogLevel("WARN") # Don't Show warning
df_pyspark=spark.read.csv('ClientData100000.csv',header=False,inferSchema=True)
df_pyspark.show()

#build
columns= ["client_id","first_name","last_name","phone_number","email_address"]
df_pyspark2 = df_pyspark.withColumnRenamed('_c0','client_id')
df_pyspark2 = df_pyspark2.withColumnRenamed('_c1','first_name')
df_pyspark2 = df_pyspark2.withColumnRenamed('_c2','last_name')
df_pyspark2 = df_pyspark2.withColumnRenamed('_c3','phone_number')
df_pyspark2 = df_pyspark2.withColumnRenamed('_c4','email_address')
df_pyspark2.printSchema()
df_pyspark2.show(truncate=False)

#print
df_pyspark2.sort(df_pyspark2.client_id.asc()).show(truncate=False)

#refine data
df_pyspark3=df_pyspark2.filter(df_pyspark2.last_name == 'Thomas')
df_pyspark3 = df_pyspark3.sort(df_pyspark3.client_id.asc())
df_pyspark3.show(truncate=False)

#dataframe to new csv
pandata = df_pyspark3.toPandas()
pandata.to_csv('Thomas.csv', index=False)