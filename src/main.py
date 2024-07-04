import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkContext, StorageLevel
from pyspark.sql.types import *
from pyspark.sql.functions import col
import plotly.express as px
from FolderUtilities import create_folder



#mode for crete folder
mode = 0o666

#drirectory project
parent_dir = "../" # il . sta per la cartella sopra (. = 1 cartella sopra, .. 2 cartelle sopra)


fld_data = create_folder(parent_dir,'data',mode)
create_folder(parent_dir,'exp_image',mode)
create_folder(parent_dir,'log',mode)



spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()



#path per il file csv da leggere
londonBike = r"C:\Users\adila\PycharmProjects\BigData_ProjectExam\data\london.csv"

#leggo il csv
df_londonBike = spark.read.option("delimiter", ",").option("header", True).csv(londonBike)
#df_londonBike.show()
df_londonBike.printSchema()

#assegna un nuovo nome alle colonne
#nuovo_df = df_londonBike.withColumnRenamed('rental_id', 'ID')

#seleczione 3 metodi:
#nuovo_df= df_londonBike.select('bike_id')
#nuovo_df= df_londonBike.select(df_londonBike['bike_id'])
#nuovo_df= df_londonBike.select(col('bike_id')).alias('nuovo nome colonna') # questa funzione è meglio delle altre perchè ciconsente di applicare anche altre funzioni

#definire il formato delle colonne
df_londonBike = df_londonBike.withColumn("duration", df_londonBike["duration"].cast("integer"))
df_londonBike = df_londonBike.withColumn("start_rental_date_time", df_londonBike["start_rental_date_time"].cast("date"))
df_londonBike = df_londonBike.withColumn("end_rental_date_time", df_londonBike["end_rental_date_time"].cast("date"))
df_londonBike.printSchema()

#verifico il numero di partizioni
print(df_londonBike.rdd.getNumPartitions())
#modifico il numero di partizioni
#df_londonBike = df_londonBike.coalesce(10)
#aumenta il numero di partiizoni
#df_londonBike = df_londonBike.repartition(100)

#funzione chache...default utilizza solo la ram
df_londonBike = df_londonBike.cache()
#iporto l'utilizzo delle ram e quando non basta più usa il disco
#storageLevel.MEMORY_AND_DISK
#storageLevel.MEMORY_ONLY
#storageLevel.DISK_ONLY
#df_londonBike = df_londonBike.persist(storageLevel.MEMORY_AND_DISK)
#toglie dalla cache
#df_londonBike = df_londonBike.unpersist(storageLevel.MEMORY_AND_DISK)
#