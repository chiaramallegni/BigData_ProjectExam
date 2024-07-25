#import librerie esterne
import shutil
from tkinter import filedialog as fd

import matplotlib.pyplot as plt
import networkx as nx

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
df_edge  = df_edge.filter(col("start_station_id") != col("end_station_id"))
#select = df_g.filter(col('count') == 0) # non esistono staioni che non sono relazionate tra loco --verificare
#df_edge = df_edge.withColumn('relationship', lit('Exist')) # fare una query inserento una coniziona tipo se count > di metti altro, altrimenti bassa o fai delle classi magari in base all'ora di star e end....ragionarci.
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

gp = nx.from_pandas_edgelist(df_edge.toPandas(),'src','dst')

graph1 = plt.figure(figsize=(10, 8))
pos = nx.kamada_kawai_layout(gp)
node_options = {"node_color": "orange", "node_size":50}
edge_options = {"width": 1.0, "alpha" : .25, "edge_color": "green"}
nx.draw_networkx_nodes(gp, pos, **node_options)
nx.draw_networkx_edges(gp, pos, **edge_options)
#plt.show()
plt.savefig((fld_image + '/graph1.png'))

gp_cent = nx.density(gp)


plt.figure(figsize=(10, 8))
pos = nx.kamada_kawai_layout(gp)
node_options = {"node_color": "orange", "node_size":50}
edge_options = {"width": 1.0, "alpha" : .25, "edge_color": "green"}
nx.draw_networkx_nodes(gp, pos, **node_options)
nx.draw_networkx_edges(gp, pos, **edge_options)
plt.show()
plt.savefig((fld_image + '/graph2.png'))

gp_betw = nx.betweenness_centrality(gp)

plt.figure(figsize=(50, 40))
#pos = nx.kamada_kawai_layout(gp)
node_options = {"node_color": "orange", "node_size":50}
edge_options = {"width": 1.0, "alpha" : .25, "edge_color": "green"}
nx.draw(gp, node_size=[v*1500 for v in gp_betw.values()], node_color= "orange", **edge_options)
plt.show()
plt.savefig((fld_image + '/graph2.png'))


plot1 = nx.draw(gp, with_labels = True)
plt.savefig((fld_image + '/graph1.png'))
cent = nx.centrality.betweenness_centrality(gp)
plot2 = nx.draw(gp,node_size=[v*1500 for v in cent.values()], edge_color='silver')
#plot = nx.draw(gp, with_labels = True)
plt.savefig((fld_image + '/graph2.png'))


#spark.stop()



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