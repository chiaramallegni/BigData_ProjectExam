from set_log import MyLogger

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from df_cleaning_and_manage import ExectuteDfManage
from pyspark.sql.functions import *
from variables_and_path import *
from utilities import Utilities

my_log = MyLogger()
my_ut = Utilities()

# STATISTICS

my_log.logger.info("-- START STATISTICS --")

spark = SparkSession.builder.master("local").appName("London Bike Statistics Analysis").getOrCreate()



## count record londonbike

df_londonBike_cl = ExectuteDfManage.manage_londonBike(londonBike, spark, my_log.logger)
df_londonBike_cl.summary()



df_londonBike_cl.createOrReplaceTempView("VIEW_df_londonBike_cl")
my_log.logger.info("Created temporary VIEW_df_londonBike_cl")

df_londonBike_cl.summary()
## check and count if rental_id id unique

### with spark.sql
#spark.sql("SELECT DISTINCT rental_id FROM VIEW_df_londonBike_cl").count()
#print(f"Numero di valori unici nella colonna 'rental_id': {unique_count}")

### with spark distinct 1
#unique_count = df_londonBike_cl.select("rental_id").distinct().count()
#print(f"Numero di valori unici nella colonna 'rental_id': {unique_count}")

### with spark distinct 2
# df_londonBike_cl.select(count_distinct(col("rental_id"))).show()


## var of duration
df_londonBike_cl.select(variance(col("duration_hour")))

## correlation between start station and end station
df_londonBike_cl.corr("start_station_id", "end_station_id")

## correlation between..... -
df_londonBike_cl.corr("start_hour", "duration_hour")
df_londonBike_cl.corr("start_hour", "start_station_id")
df_londonBike_cl.corr("end_hour", "end_station_id")


## median for duration in minutes
median_duration_hour = df_londonBike_cl.agg(median("duration_min")).collect()[0][0]

## Most used stations related to median duration
df_londonBike_cl.filter((col("duration_hour")) > median_duration_hour).select(col("start_station_id"), col("end_station_id")).show(10)


spark.sparkContext.stop()

