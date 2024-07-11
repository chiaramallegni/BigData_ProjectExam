#import librerie esterne
import shutil
from tkinter import filedialog as fd
from pyspark.sql import SparkSession
from pyspark import SparkContext, StorageLevel
from pyspark.sql.types import *
from pyspark.sql.functions import *

import plotly.express as px
from graphframes import GraphFrame
from shapely.geometry import Point, Polygon, shape # creating geospatial data
from shapely import wkb, wkt # creating and parsing geospatial data

# import classi interne

from variables_and_path import *

# in variables_and_path creo le cartelle necessarie al progetto
# recupera tramite richiesta il file contente i dati zippati che devono essere copaiati nella directory data del progetto

if os.path.isdir(data_subfoler):
    print("i csv from sono già presenti")
else:
    print("chiedo la cartella dei dat da scaricati da importare nel progetto")
    source_data_file = fd.askopenfilename()
    # copia e estrare nella directory data del progetto
    shutil.copy(source_data_file, fld_data)
    print("unzip del file")
    # unzip del file
    shutil.unpack_archive(source_data_file, fld_data, 'zip')
    print("file estratto")

# importo i dati geografici creati
if os.path.exists(data_subfoler + "gfd_buildings_200m.csv"):
   print("i csv from geodata sono stati già generati")
else:
   print("genero geodata sono stati già generati con manage_geodata")
   from manage_geodata import *


spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

sdf_buildings_200m = spark.read.option("delimiter", ",").option("header", True).csv(data_subfoler + "gfd_buildings_200m.csv")
sdf_london_pois_200m = spark.read.option("delimiter", ",").option("header", True).csv(data_subfoler + "gdf_london_pois_200m.csv")
sdf_london_railway_station_200m = spark.read.option("delimiter", ",").option("header", True).csv(data_subfoler + "gdf_london_railway_station_200m.csv")

sdf_buildings_200m.printSchema()
sdf_london_pois_200m.printSchema()
sdf_london_railway_station_200m.printSchema()

#leggo il csv
df_londonBike = spark.read.option("delimiter", ",").option("header", True).csv(londonBike)
#df_londonBike.show()

#definire il formato delle colonne
df_londonBike = df_londonBike.withColumn("duration", df_londonBike["duration"].cast("integer"))
df_londonBike = df_londonBike.withColumn("start_station_id", df_londonBike["start_station_id"].cast("integer"))
df_londonBike = df_londonBike.withColumn("end_station_id", df_londonBike["end_station_id"].cast("integer"))
df_londonBike = df_londonBike.withColumn("start_rental_date_time", to_timestamp(col("start_rental_date_time")))
df_londonBike = df_londonBike.withColumn("end_rental_date_time", to_timestamp(col("end_rental_date_time")))

#scompongo le colonne timestamp in giorno-ore-minuti-secondi in modo che mi consentano di fare delle analisi staistiche sul tempo
df_londonBike = df_londonBike.withColumn("start_day", to_date(col("start_rental_date_time")))
df_londonBike = df_londonBike.withColumn('start_day_of_week', date_format('start_day', 'EEEE'))
df_londonBike = df_londonBike.withColumn('start_month', date_format('start_day', 'MMMM'))
df_londonBike = df_londonBike.withColumn("start_hour", hour(col("start_rental_date_time")))
df_londonBike = df_londonBike.withColumn("start_minute", minute(col("start_rental_date_time")))
#df_londonBike = df_londonBike.withColumn("start_second", second(col("start_rental_date_time")))
df_londonBike = df_londonBike.withColumn("end_day", to_date(col("end_rental_date_time")))
df_londonBike = df_londonBike.withColumn('end_day_of_week', date_format('end_day', 'EEEE'))
df_londonBike = df_londonBike.withColumn('end_month', date_format('start_day', 'MMMM'))
df_londonBike = df_londonBike.withColumn("end_hour", hour(col("end_rental_date_time")))
df_londonBike = df_londonBike.withColumn("end_minute", minute(col("end_rental_date_time")))
# = df_londonBike.withColumn("end_second", second(col("end_rental_date_time")))


df_londonBike.orderBy(col("start_hour"),col("end_hour").desc()).show()

df_londonBike.printSchema()


df_date = df_londonBike.select(col("start_rental_date_time"), col("end_rental_date_time"))
df_date.show()

sdf_london_pois_200mgp = sdf_london_pois_200m.groupBy(col('station_id'), col('fclass')).count()

#creazione del grafo

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

#spark.stop()


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

#iporto l'utilizzo delle ram e quando non basta più usa il disco
#storageLevel.MEMORY_AND_DISK
#storageLevel.MEMORY_ONLY
#storageLevel.DISK_ONLY
#df_londonBike = df_londonBike.persist(storageLevel.MEMORY_AND_DISK)
#toglie dalla cache
#df_londonBike = df_londonBike.unpersist(StorageLevel.MEMORY_AND_DISK)
#