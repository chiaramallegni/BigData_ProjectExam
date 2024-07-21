#import librerie esterne
import shutil
from tkinter import filedialog as fd
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
import set_log

set_log.logger.info("-- SPARK SESSION --")

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
set_log.logger.info("Avviata sessione spark")


#from espl_london_bike import *

df_londonBike = spark.read.option("delimiter", ",").option("header", True).csv(londonBike)
set_log.logger.info("TOP row for LondonBike  \n"+ getShowStringForLog(df_londonBike))

# verifyng number of partition
PartitionNumber = df_londonBike.rdd.getNumPartitions()
set_log.logger.info("Partition Number for df_londonBike is: " + str(PartitionNumber))

#definire il formato delle colonne
df_londonBike = df_londonBike.withColumn("duration", df_londonBike["duration"].cast("double"))
df_londonBike = df_londonBike.withColumn("rental_id", df_londonBike["rental_id"].cast("integer"))
df_londonBike = df_londonBike.withColumn("bike_id", df_londonBike["bike_id"].cast("integer"))
df_londonBike = df_londonBike.withColumn("start_station_id", df_londonBike["start_station_id"].cast("integer"))
df_londonBike = df_londonBike.withColumn("end_station_id", df_londonBike["end_station_id"].cast("integer"))
df_londonBike = df_londonBike.withColumn("start_rental_date_time", to_timestamp(col("start_rental_date_time")))
df_londonBike = df_londonBike.withColumn("end_rental_date_time", to_timestamp(col("end_rental_date_time")))

#scompongo le colonne timestamp in modo che mi consentano di fare delle analisi staistiche sul tempo
df_londonBike = df_londonBike.withColumn("start_day", to_date(col("start_rental_date_time")))
df_londonBike = df_londonBike.withColumn('start_day_of_week', date_format('start_day', 'EEEE'))
df_londonBike = df_londonBike.withColumn('start_month', date_format('start_day', 'MMMM'))
df_londonBike = df_londonBike.withColumn("start_hour", hour(col("start_rental_date_time")))
df_londonBike = df_londonBike.withColumn("start_minute", minute(col("start_rental_date_time")))
df_londonBike = df_londonBike.withColumn("end_day", to_date(col("end_rental_date_time")))
df_londonBike = df_londonBike.withColumn('end_day_of_week', date_format('end_day', 'EEEE'))
df_londonBike = df_londonBike.withColumn('end_month', date_format('start_day', 'MMMM'))
df_londonBike = df_londonBike.withColumn("end_hour", hour(col("end_rental_date_time")))
df_londonBike = df_londonBike.withColumn("end_minute", minute(col("end_rental_date_time")))
df_londonBike = df_londonBike.withColumn('duration_min',round(col('duration')/60))
df_londonBike = df_londonBike.withColumn('duration_hour',round(col('duration')/3600))
df_londonBike = df_londonBike.withColumn('duration_day',round(col('duration')/86400))


# rimozione valori nulli
df_londonBike_cl = df_londonBike.dropna()
df_londonBike_cl = df_londonBike_cl.withColumn('end_station_name', trim(col('end_station_name')))
df_londonBike_cl = df_londonBike_cl.withColumn('start_station_name', trim(col('start_station_name')))
#rimozione spazi

# crete new column in order to figure the time_of_day
df_londonBike_cl = df_londonBike_cl.withColumn("time_of_day",
                            when((col("start_hour") > 5) & (col("start_hour") <= 11), "morning").\
                            when((col("start_hour") > 11) & (col("start_hour") <= 14), "lunch").\
                            when((col("start_hour") > 14) & (col("start_hour") <= 18), "afternoon").\
                            when((col("start_hour") > 18) & (col("start_hour") <= 22), "dinner").\
                            when(((col("start_hour") > 22) & (col("start_hour") <= 24)) | ((col("start_hour") >= 0) & (col("start_hour") <= 5)), "night").\
                            otherwise('n.a.'))

set_log.logger.info("TOP row for LondonBike Cleaned and Updated  \n"+ getShowStringForLog(df_londonBike))

# print schema df updated
print(df_londonBike_cl.printSchema())

## stampo le statistiche del set di dati pulito
#df_londonBike_cl_sum = df_londonBike_cl.summary()
#df_londonBike_cl_sum.show()

df_londonBike = df_londonBike.repartition(70)

# crete view
df_londonBike_cl.createOrReplaceTempView("VIEW_df_londonBike_cl")
set_log.logger.info("Created temporary VIEW_df_londonBike_cl")

# STATISTICS
set_log.logger.info("-- START STATISTICS --")

## count record londonbike
df_londonBike_cnt = df_londonBike.count()
set_log.logger.info("London Bike Count is: " + str(df_londonBike_cnt))

## check and count if rental_id id unique

### with spark.sql
#spark.sql("SELECT DISTINCT rental_id FROM VIEW_df_londonBike_cl").count()
#print(f"Numero di valori unici nella colonna 'rental_id': {unique_count}")

### with spark distinct 1
#unique_count = df_londonBike_cl.select("rental_id").distinct().count()
#print(f"Numero di valori unici nella colonna 'rental_id': {unique_count}")

#### with spark distinct 2
#df_londonBike_cl.select(count_distinct(col("rental_id"))).show()


## kurtosi of duration
df_kurtosis_duration = df_londonBike_cl.select(kurtosis(col("duration_hour")))
set_log.logger.info("London Bike kurtosis duration is  \n"+ getShowStringForLog(df_kurtosis_duration))
## skunnes of duration
df_skewness_duration = df_londonBike_cl.select(skewness(col("duration_hour")))
set_log.logger.info("London Bike skewness is  \n"+ getShowStringForLog(df_skewness_duration))
## standard error of duration
df_stddev_duration = df_londonBike_cl.select(stddev(col("duration_hour")))
set_log.logger.info("London Bike stddev is  \n"+ getShowStringForLog(df_stddev_duration))
## var of duration
df_variance_duration= df_londonBike_cl.select(variance(col("duration_hour")))
set_log.logger.info("London Bike variance is  \n"+ getShowStringForLog(df_variance_duration))

## correlation between start station and end station - result:  0.2531953725202463
df_londonBike_cl.corr("start_station_id", "end_station_id")
## correlation between..... - rivedere
df_londonBike_cl.corr("start_hour", "duration_hour")
df_londonBike_cl.corr("start_hour", "start_station_id")
df_londonBike_cl.corr("end_hour", "end_station_id")


## TOP 30 link most frequently used station link

top30_link_cnt = spark.sql("SELECT COUNT(*) AS link_cnt, CONCAT(start_station_id,  '-' , end_station_id) AS link "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_station_id, end_station_id "
                          "ORDER BY link_cnt DESC LIMIT 30")

PL_bar_top30_link_cnt = px.bar(top30_link_cnt, x='link', y='link_cnt', text_auto = True, labels = 'link', title = 'TOP 30 Link Station Count', color = 'link_cnt', color_continuous_scale='Bluered')
PL_bar_top30_link_cnt.write_html(fld_image + '/top30_link_count.html')

set_log.logger.info("Plotted TOP 30 link count")

## TOP 30 start start station most frequently used start station

top30_start_st_cnt = spark.sql("SELECT COUNT(*) AS start_station_cnt, start_station_name "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_station_name "
                          "ORDER BY start_station_cnt DESC LIMIT 30")

PL_bar_top30_start_st_cnt = px.bar(top30_start_st_cnt, x='start_station_name', y='start_station_cnt', text_auto = True, labels = 'start_station_name', title = 'TOP 30 Start Station Count', color = 'start_station_cnt', color_continuous_scale='Bluered')
PL_bar_top30_start_st_cnt.write_html(fld_image + '/top30_start_st_count.html')

set_log.logger.info("Plotted TOP 30 Start Station count")

## TOP 30 end start station most frequently used start station

top30_end_st_cnt = spark.sql("SELECT COUNT(*) AS end_station_cnt, end_station_name "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY end_station_name "
                          "ORDER BY end_station_cnt DESC LIMIT 30")

PL_bar_top30_end_st_cnt = px.bar(top30_end_st_cnt, x='end_station_name', y='end_station_cnt', text_auto = True, labels = 'end_station_name', title = 'TOP 30 End Station Count', color = 'end_station_cnt', color_continuous_scale='Bluered')
PL_bar_top30_end_st_cnt.write_html(fld_image + '/top30_start_end_count.html')

set_log.logger.info("Plotted TOP 30 End Station count")

## Day Week Count
st_day_week_cnt = spark.sql("SELECT COUNT(*) AS day_week_cnt,  start_day_of_week "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_day_of_week "
                          "ORDER BY day_week_cnt DESC ")

PL_bar_day_week_cnt = px.bar(st_day_week_cnt, x='start_day_of_week', y='day_week_cnt', text_auto = True, labels = 'start_day_of_week', title = 'Start Day of Week Count', color = 'day_week_cnt', color_continuous_scale='Bluered')
PL_bar_day_week_cnt.write_html(fld_image + '/st_day_week_count.html')

set_log.logger.info("Plotted Start Day Week Count")

## Month Count
st_month_cnt = spark.sql("SELECT COUNT(*) start_month_cnt,  start_month "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_month "
                          "ORDER BY start_month_cnt DESC ")

PL_bar_st_month_cnt = px.bar(st_month_cnt, x='start_month', y='start_month_cnt', text_auto = True, labels = 'start_month', title = 'Start Month Count', color = 'start_month_cnt', color_continuous_scale='Bluered')
PL_bar_st_month_cnt.write_html(fld_image + '/st_month_count.html')

set_log.logger.info("Plotted Start Month Week Count")

## Day Week Frequency
st_day_week_frq = st_day_week_cnt.withColumn('frequency_day', col("day_week_cnt") / df_londonBike_cnt)
set_log.logger.info("Day Week Frequency is  \n"+ getShowStringForLog(st_day_week_frq))

## MonthFrequency
st_month_frq = st_month_cnt.withColumn('frequency_month', col("start_month_cnt") / df_londonBike_cnt)
set_log.logger.info("Month Frequency is   \n"+ getShowStringForLog(st_month_frq))

#voglio creare delle fascie orarie e vedere in quale fascia oraria vi è più utilizzo de servizio  #da capire come mettere l'orario #fallocon with column
#crea una colonna fascia oraria


## median for duration in minutes (le ore erano a 0)
median_duration_hour = df_londonBike_cl.agg(median("duration_min")).collect()[0][0]

# ho preso le stazioni più utilizzate legate alla duration
max_duration = df_londonBike_cl.filter((col("duration_hour")) > median_duration_hour).select(col("start_station_id"), col("end_station_id"))
set_log.logger.info("Max Duration Station is  \n"+ getShowStringForLog(max_duration))

mean_month_duration = df_londonBike_cl.groupBy("start_month").avg("duration_hour")
mean_month_duration = mean_month_duration.withColumnRenamed('avg(duration_hour)', 'mean_month_duration_hour')
mean_month_duration = mean_month_duration.orderBy('mean_month_duration_hour')

PL_bar_month_duration_mean = px.bar(mean_month_duration, x='start_month', y='mean_month_duration_hour', text_auto = True, labels = 'start_month', title = 'Mean Duration for each Month', color = 'mean_month_duration_hour', color_continuous_scale='Bluered')
PL_bar_month_duration_mean.write_html(fld_image + '/month_duration_mean.html')

set_log.logger.info("Plotted Mean Dusration for each Month")


# drop temporary view
spark.catalog.dropTempView("VIEW_df_londonBike_cl")

set_log.logger.info("Droped temporary VIEW_df_londonBike_cl")


set_log.logger.info ("-- JOIN BLONDON BIKE GEODATA--")

# importo i dati geografici creati
if os.path.exists(data_subfoler + "gfd_buildings_200m.csv"):
   print("i csv from geodata sono stati già generati")
else:
   print("genero geodata sono stati già generati con manage_geodata")
   from manage_geodata import *

sdf_buildings_200m = spark.read.option("delimiter", ",").option("header", True).csv(data_subfoler + "gfd_buildings_200m.csv")
sdf_london_pois_200m = spark.read.option("delimiter", ",").option("header", True).csv(data_subfoler + "gdf_london_pois_200m.csv")
sdf_london_railway_station_200m = spark.read.option("delimiter", ",").option("header", True).csv(data_subfoler + "gdf_london_railway_station_200m.csv")

sdf_buildings_200m.printSchema()
sdf_london_pois_200m.printSchema()
sdf_london_railway_station_200m.printSchema()


station_poist_cnt = sdf_london_pois_200m.groupBy(col('fclass'), col('station_id')).count().orderBy(col('station_id').desc())
#station_poist_cnt.show()
station_poist_dst = sdf_london_pois_200m.select(col("fclass")).distinct().show()

station_poist_dst = sdf_london_pois_200m.distinct("fclass").count().show()

sdf_london_pois_200m.createOrReplaceTempView("V_london_pois_200m")
spark.sql("SELECT DISTINCT fclass FROM V_london_pois_200m").count()
station_poist_dst.count()

sdf_london_pois_200mgp = sdf_london_pois_200m.groupBy(col('station_id'), col('fclass')).count()

