
from pyspark.sql.functions import *
from utilities import Utilities



class ExectuteDfManage:

    @staticmethod
    def manage_londonBike(londonBike, spark, logger):

        df_londonBike = spark.read.option("delimiter", ",").option("header", True).csv(londonBike)
        logger.info("TOP row for LondonBike  \n"+ Utilities.getShowStringForLog(df_londonBike))

        # checking number of partition


        # definition of the column format
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


        # create new column in order to figure the time_of_day
        df_londonBike_cl = df_londonBike_cl.withColumn("start_time_of_day",
                                    when((col("start_hour") > 5) & (col("start_hour") <= 11), "morning").\
                                    when((col("start_hour") > 11) & (col("start_hour") <= 14), "lunch").\
                                    when((col("start_hour") > 14) & (col("start_hour") <= 18), "afternoon").\
                                    when((col("start_hour") > 18) & (col("start_hour") <= 22), "dinner").\
                                    when(((col("start_hour") > 22) & (col("start_hour") <= 24)) | ((col("start_hour") >= 0) & (col("start_hour") <= 5)), "night").\
                                    otherwise('n.a.'))
        df_londonBike_cl = df_londonBike_cl.withColumn("end_time_of_day",
                                    when((col("end_hour") > 5) & (col("end_hour") <= 11), "morning").\
                                    when((col("end_hour") > 11) & (col("end_hour") <= 14), "lunch").\
                                    when((col("end_hour") > 14) & (col("end_hour") <= 18), "afternoon").\
                                    when((col("end_hour") > 18) & (col("end_hour") <= 22), "dinner").\
                                    when(((col("end_hour") > 22) & (col("end_hour") <= 24)) | ((col("start_hour") >= 0) & (col("start_hour") <= 5)), "night").\
                                    otherwise('n.a.'))

        logger.info("TOP row for LondonBike Cleaned and Updated  \n"+ Utilities.getShowStringForLog(df_londonBike))

        # print schema df updated
        print(df_londonBike_cl.printSchema())

        return df_londonBike_cl


