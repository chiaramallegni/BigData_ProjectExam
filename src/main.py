#import librerie esterne
import shutil
from tkinter import filedialog as fd
from pyspark.sql import SparkSession
from pyspark import SparkContext, StorageLevel
from pyspark.sql.types import *
from pyspark.sql.functions import col
import plotly.express as px
from graphframes import GraphFrame

# import classi interne

from variables_and_path import *

# in variables_and_path creo le cartelle necessarie al progetto

# recupera tramite richiesta il file contente i dati zippati che devono essere copaiati nella directory data del progetto
source_data_file = fd.askopenfilename()

# copia e estrare nella directory data del progetto
shutil.copy(source_data_file, fld_data)
print("unzip del file")
#unzip del file
shutil.unpack_archive(source_data_file, fld_data, 'zip')
print("file estratto")


# importo i dati geografici creati

from manage_geodata import *
from convert_utilities import geometry_to_wkt

gdf_list = [gdf_london_stations, gfd_buildings_200m, gdf_london_pois_200m]

df_london_stations = gdf_london_stations.copy()
df_buildings_200m = gfd_buildings_200m.copy()
df_london_pois_200m = gdf_london_pois_200m.copy()

gdf_list = [df_london_stations, df_buildings_200m, df_london_pois_200m]

for gdf in gdf_list:
    geometry_to_wkt(gdf)
    print('converisone wkt', gdf, 'effettuata')

# avvio la sessione di spark
spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# creazione datafame spark from geopandas

sdf_london_stations = spark.createDataFrame(df_london_stations)
print('creato spark sdf_london_stations')
sdf_london_stations.printSchema()

sdf_buildings_200m = spark.createDataFrame(df_buildings_200m)
print('creato spark sdf_buildings_200m')
sdf_buildings_200m.printSchema()

sdf_london_pois_200m = spark.createDataFrame(df_london_pois_200m)
print('creato spark sdf_london_pois_200m')
sdf_london_pois_200m.printSchema()



#leggo il csv
df_londonBike = spark.read.option("delimiter", ",").option("header", True).csv(londonBike)
df_londonBike.show(5)
df_londonBike.printSchema()

#definire il formato delle colonne
f_londonBike = df_londonBike.withColumn("duration", df_londonBike["duration"].cast("integer"))
df_londonBike = df_londonBike.withColumn("start_rental_date_time", df_londonBike["start_rental_date_time"].cast("date"))
df_londonBike = df_londonBike.withColumn("end_rental_date_time", df_londonBike["end_rental_date_time"].cast("date"))
df_londonBike.printSchema()




#assegna un nuovo nome alle colonne
#nuovo_df = df_londonBike.withColumnRenamed('rental_id', 'ID')

#seleczione 3 metodi:
#nuovo_df= df_londonBike.select('bike_id')
#nuovo_df= df_londonBike.select(df_londonBike['bike_id'])
#nuovo_df= df_londonBike.select(col('bike_id')).alias('nuovo nome colonna') # questa funzione è meglio delle altre perchè ciconsente di applicare anche altre funzioni



#verifico il numero di partizioni
#print(df_londonBike.rdd.getNumPartitions())
#modifico il numero di partizioni
#df_londonBike = df_londonBike.coalesce(10)
#aumenta il numero di partiizoni
#df_londonBike = df_londonBike.repartition(100)

#funzione chache...default utilizza solo la ram
#df_londonBike = df_londonBike.cache()
#iporto l'utilizzo delle ram e quando non basta più usa il disco
#storageLevel.MEMORY_AND_DISK
#storageLevel.MEMORY_ONLY
#storageLevel.DISK_ONLY
#df_londonBike = df_londonBike.persist(storageLevel.MEMORY_AND_DISK)
#toglie dalla cache
#df_londonBike = df_londonBike.unpersist(storageLevel.MEMORY_AND_DISK)
#