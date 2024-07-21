#import librerie esterne
import shutil
from tkinter import filedialog as fd
import sys
import logging
import datetime
import os


from pyspark.sql import SparkSession
from pyspark import SparkContext, StorageLevel
from pyspark.sql.types import *
from pyspark.sql.functions import *

import plotly.express as px

from graphframes import GraphFrame
from shapely.geometry import Point, Polygon, shape # creating geospatial data
from shapely import wkb, wkt # creating and parsing geospatial data

# import classi interne
import variables_and_path
from variables_and_path import *
from set_log import *

# in variables_and_path creo le cartelle necessarie al progetto
# recupera tramite richiesta il file contente i dati zippati che devono essere copaiati nella directory data del progetto

logger.info("******LONDON BIKE *******")
logger.info("")
logger.info("Stating Analysis...")

logger.info ("-- IMPORT DATA --")

if os.path.isdir(data_subfoler):
    print("i csv from sono già presenti")
    logger.info("i csv from sono già presenti")
else:
    print("chiedo la cartella dei dat da scaricati da importare nel progetto")
    logger.info("chiedo la cartella dei dat da scaricati da importare nel progetto")
    source_data_file = fd.askopenfilename()
    # copia e estrare nella directory data del progetto
    shutil.copy(source_data_file, fld_data)
    print("unzip del file")
    logger.info("unzip del file")
    # unzip del file
    shutil.unpack_archive(source_data_file, fld_data, 'zip')
    print("file estratto")
    logger.info("ile estratto")



logger.info("-- START MANAGE LONDON BIKE DATAFRAME --")

from manage_london_bike import *

set_log.logger.info("-- START GRAPH FRAME --")

# GRAPHFRAME

df_edge = df_londonBike.groupBy(col('start_station_id'), col('end_station_id')).count().alias('bike_run_count')
#select = df_g.filter(col('count') == 0) # non esistono staioni che non sono relazionate tra loco.
df_edge = df_edge.withColumn('relationship', lit('Exist')) # fare una query inserento una coniziona tipo se count > di metti altro, altrimenti bassa o fai delle classi magari in base all'ora di star e end....ragionarci.
# oppure ancora quando fai le join con la prrte geografica inventarci qualcosa ....dare in significato alla colonna relationship....
#assegna un nuovo nome alle colonne
df_edge = df_edge.withColumnRenamed('start_station_id', 'src')
df_edge = df_edge.withColumnRenamed('end_station_id', 'dst')
df_edge = df_edge.withColumnRenamed('count', 'weight')

src = df_edge.select('src')
dst = df_edge.select('dst')
df_node = src.union(dst).distinct().withColumnRenamed('src', 'id')
g_london_stations = GraphFrame(df_node, df_edge)
g_london_stations.inDegrees.show()
g_london_stations.outDegrees.show()
g_london_stations.degrees.show()
spark.stop()


#nuovo_df = df_londonBike.withColumnRenamed('rental_id', 'ID')

#seleczione 3 metodi:
#nuovo_df= df_londonBike.select('bike_id')
#nuovo_df= df_londonBike.select(df_londonBike['bike_id'])
#nuovo_df= df_londonBike.select(col('bike_id')).alias('nuovo nome colonna') # questa funzione è meglio delle altre perchè ciconsente di applicare anche altre funzioni




#modifico il numero di partizioni
#df_londonBike = df_londonBike.coalesce(10)
#aumenta il numero di partiizoni
#df_londonBike = df_londonBike.repartition(100)

#funzione chache...default utilizza solo la ram

#iporto l'utilizzo delle ram e quando non basta più usa il disco
#storageLevel.MEMORY_AND_DISK
#storageLevel.MEMORY_ONLY
#storageLevel.DISK_ONLY
#df_londonBike = df_londonBike.persist(storageLevel.MEMORY_AND_DISK)
#toglie dalla cache
#df_londonBike = df_londonBike.unpersist(StorageLevel.MEMORY_AND_DISK)
#