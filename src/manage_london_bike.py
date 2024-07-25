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
from variables_and_path import *
import set_log
import utilities

set_log.logger.info("-- SPARK SESSION --")

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
set_log.logger.info("Spark Session started")


df_londonBike = spark.read.option("delimiter", ",").option("header", True).csv(londonBike)
set_log.logger.info("TOP row for LondonBike  \n"+ getShowStringForLog(df_londonBike))

# checking number of partition
PartitionNumber = df_londonBike.rdd.getNumPartitions()
set_log.logger.info("Partition Number for df_londonBike is: " + str(PartitionNumber))

# define the format of the columns
df_londonBike = df_londonBike.withColumn("duration", df_londonBike["duration"].cast("double"))
df_londonBike = df_londonBike.withColumn("rental_id", df_londonBike["rental_id"].cast("integer"))
df_londonBike = df_londonBike.withColumn("bike_id", df_londonBike["bike_id"].cast("integer"))
df_londonBike = df_londonBike.withColumn("start_station_id", df_londonBike["start_station_id"].cast("integer"))
df_londonBike = df_londonBike.withColumn("end_station_id", df_londonBike["end_station_id"].cast("integer"))
df_londonBike = df_londonBike.withColumn("start_rental_date_time", to_timestamp(col("start_rental_date_time")))
df_londonBike = df_londonBike.withColumn("end_rental_date_time", to_timestamp(col("end_rental_date_time")))

# time stamp column decomposition for statistical analysis on time
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


# remove the null values
df_londonBike_cl = df_londonBike.dropna()

# remove spaces
df_londonBike_cl = df_londonBike_cl.withColumn('end_station_name', trim(col('end_station_name')))
df_londonBike_cl = df_londonBike_cl.withColumn('start_station_name', trim(col('start_station_name')))


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

# print the statistics of the clean data set
# df_londonBike_cl_sum = df_londonBike_cl.summary()
# df_londonBike_cl_sum.show()

# modifing repartition
df_londonBike_cl = df_londonBike_cl.repartition(70)

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

### with spark distinct 2
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

## correlation between start station and end station
st_end_corr= df_londonBike_cl.corr("start_station_id", "end_station_id")
set_log.logger.info("Start and End Station correlation is  \n"+ str(st_end_corr))

## correlation between..... - rivedere
df_londonBike_cl.corr("start_hour", "duration_hour")
df_londonBike_cl.corr("start_hour", "start_station_id")
df_londonBike_cl.corr("end_hour", "end_station_id")


## TOP 30 link most frequently used station link

top30_link_cnt = spark.sql("SELECT COUNT(*) AS link_cnt, CONCAT(start_station_id,  '-' , end_station_id) AS link "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_station_id, end_station_id "
                          "ORDER BY link_cnt DESC LIMIT 30")

PL_bar_top30_link_cnt = px.bar(top30_link_cnt, x='link', y='link_cnt',
                               text_auto = True, labels = 'link',
                               title = 'TOP 30 Link Station Count',
                               color = 'link_cnt',
                               color_continuous_scale='Bluered')

PL_bar_top30_link_cnt.write_html(fld_image + '/top30_link_count.html')
#PL_bar_top30_link_cnt.write_image(fld_image + '/top30_link_count.png')

set_log.logger.info("Plotted TOP 30 link count")

## TOP 30 start station most frequently used

top30_start_st_cnt = spark.sql("SELECT COUNT(*) AS start_station_cnt, start_station_name "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_station_name "
                          "ORDER BY start_station_cnt DESC LIMIT 30")

PL_bar_top30_start_st_cnt = px.bar(top30_start_st_cnt, x='start_station_name', y='start_station_cnt',
                                   text_auto = True,
                                   labels = 'start_station_name',
                                   title = 'TOP 30 Start Station Count',
                                   color = 'start_station_cnt',
                                   color_continuous_scale='Bluered')

PL_bar_top30_start_st_cnt.write_html(fld_image + '/top30_start_st_count.html')
#PL_bar_top30_start_st_cnt.write_image(fld_image + '/top30_start_st_count.png')

set_log.logger.info("Plotted TOP 30 Start Station count")

## TOP 30 end start station most frequently used

top30_end_st_cnt = spark.sql("SELECT COUNT(*) AS end_station_cnt, end_station_name "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY end_station_name "
                          "ORDER BY end_station_cnt DESC LIMIT 30")

PL_bar_top30_end_st_cnt = px.bar(top30_end_st_cnt, x='end_station_name', y='end_station_cnt',
                                 text_auto = True,
                                 labels = 'end_station_name',
                                 title = 'TOP 30 End Station Count',
                                 color = 'end_station_cnt',
                                 color_continuous_scale='Bluered')

PL_bar_top30_end_st_cnt.write_html(fld_image + '/top30_start_end_count.html')
#PL_bar_top30_end_st_cnt.write_image(fld_image + '/top30_start_end_count.png')

set_log.logger.info("Plotted TOP 30 End Station count")

## Day Week Count
st_day_week_cnt = spark.sql("SELECT COUNT(*) AS day_week_cnt,  start_day_of_week "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_day_of_week "
                          "ORDER BY day_week_cnt DESC ")

PL_bar_day_week_cnt = px.bar(st_day_week_cnt, x='start_day_of_week', y='day_week_cnt',
                             text_auto = True,
                             labels = 'start_day_of_week',
                             title = 'Start Day of Week Count',
                             color = 'day_week_cnt',
                             color_continuous_scale='Bluered')

PL_bar_day_week_cnt.write_html(fld_image + '/st_day_week_count.html')
#PL_bar_day_week_cnt.write_image(fld_image + '/st_day_week_count.png')

set_log.logger.info("Plotted Start Day Week Count")

## Month Count
st_month_cnt = spark.sql("SELECT COUNT(*) start_month_cnt,  start_month "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_month "
                          "ORDER BY start_month_cnt DESC ")

PL_bar_st_month_cnt = px.bar(st_month_cnt, x='start_month', y='start_month_cnt',
                             text_auto = True,
                             labels = 'start_month',
                             title = 'Start Month Count',
                             color = 'start_month_cnt',
                             color_continuous_scale='Bluered')

PL_bar_st_month_cnt.write_html(fld_image + '/st_month_count.html')
#PL_bar_st_month_cnt.write_image(fld_image + '/st_month_count.png')

set_log.logger.info("Plotted Start Month Count")

## Day Week Frequency
st_day_week_frq = st_day_week_cnt.withColumn('frequency_day', col("day_week_cnt") / df_londonBike_cnt)
set_log.logger.info("Day Week Frequency is  \n"+ getShowStringForLog(st_day_week_frq))

## Month Frequency
st_month_frq = st_month_cnt.withColumn('frequency_month', col("start_month_cnt") / df_londonBike_cnt)
set_log.logger.info("Month Frequency is   \n"+ getShowStringForLog(st_month_frq))

## Date cnt
date_cnt = spark.sql("SELECT COUNT(*) start_day_cnt,  start_day "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_day "
                        "ORDER BY start_day")
date_cnt.show()
PL_line_date_cnt = px.line(date_cnt, x="start_day", y="start_day_cnt",
                             title = 'Count Day Runs Over Time',
                            hover_data= {"start_day":"|%B %d, %Y"})
PL_line_date_cnt.update_traces(mode="markers",
                           marker_size=7,
                           marker_color="red"
                           #line_color = "black")
                               )
PL_line_date_cnt.update_xaxes(dtick="M1",
                         tickformat="%b\%Y",
                         ticks="inside",
                         ticklen=2)

PL_line_date_cnt.write_html(fld_image + '/cnt_day_runs_over_time.html')
#PL_line_date_cnt.write_image(fld_image + '/cnt_day_runs_over_time.png')

set_log.logger.info("Ploted Count Day Runs Over Time")


## median for duration in minutes
median_duration_hour = df_londonBike_cl.agg(median("duration_min")).collect()[0][0]

## Most used stations related to median duration
max_duration = df_londonBike_cl.filter((col("duration_hour")) > median_duration_hour).select(col("start_station_id"), col("end_station_id"))
set_log.logger.info("Max Duration Station is  \n"+ getShowStringForLog(max_duration))

## Mean duration for each month
mean_month_duration = df_londonBike_cl.groupBy("start_month").avg("duration_hour")
mean_month_duration = mean_month_duration.withColumnRenamed('avg(duration_hour)', 'mean_month_duration_hour')
mean_month_duration = mean_month_duration.orderBy('mean_month_duration_hour')

PL_bar_month_duration_mean = px.bar(mean_month_duration, x='start_month', y='mean_month_duration_hour',
                                    text_auto = True,
                                    labels = 'start_month',
                                    title = 'Mean Duration for each Month',
                                    color = 'mean_month_duration_hour',
                                    color_continuous_scale='Bluered')

PL_bar_month_duration_mean.write_html(fld_image + '/month_duration_mean.html')
#PL_bar_month_duration_mean.write_image(fld_image + '/month_duration_mean.png')

set_log.logger.info("Plotted Mean Duration for each Month")

# drop temporary view
spark.catalog.dropTempView("VIEW_df_londonBike_cl")

set_log.logger.info("Droped temporary VIEW_df_londonBike_cl")


set_log.logger.info("-- JOIN BLONDON BIKE GEODATA--")

# Import geo data
if os.path.exists(data_subfoler + "gfd_buildings_200m.csv"):
   set_log.logger.info("The geodata csv already created")
else:
   set_log.logger.info(" start create create and manage geodata ")
   from manage_geodata import *

# crete df geo data
sdf_buildings_200m = spark.read.option("delimiter", ",").option("header", True).csv(data_subfoler + "gfd_buildings_200m.csv")
sdf_london_pois_200m = spark.read.option("delimiter", ",").option("header", True).csv(data_subfoler + "gdf_london_pois_200m.csv")
sdf_london_railway_station_200m = spark.read.option("delimiter", ",").option("header", True).csv(data_subfoler + "gdf_london_railway_station_200m.csv")

sdf_buildings_200m.printSchema()
sdf_london_pois_200m.printSchema()
sdf_london_railway_station_200m.printSchema()

# crete df geo data
station_pois_cnt = sdf_london_pois_200m.groupBy(col('fclass'), col('station_id')).count().orderBy(col('station_id').desc())
#station_poist_cnt.show()
station_pois_dst = sdf_london_pois_200m.select(col("fclass")).distinct()


sdf_london_pois_200mgp = sdf_london_pois_200m.groupBy(col('station_id'), col('fclass')).count()

