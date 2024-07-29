#import librerie esterne
import shutil
from tkinter import filedialog as fd


from pyspark.sql import SparkSession
from pyspark import SparkContext, StorageLevel
from pyspark.sql.functions import *

# import classi interne

from set_log import MyLogger
from df_cleaning_and_manage import ExectuteDfManage
from gdf_cleaning_and_manage import ExectuteGDfManage

from buffer_spatial_join import Buffer_sp
from variables_and_path import *

my_log = MyLogger()

my_log.logger.info("******LONDON BIKE *******")
my_log.logger.info("")
my_log.logger.info("Stating Analysis...")

my_log.logger.info ("-- IMPORT DATA --")

# load data
if os.path.isdir(data_subfoler):
    my_log.logger.info("csv already loaded")
else:
    print("select folder data to import")
    source_data_file = fd.askopenfilename()
    shutil.copy(source_data_file, fld_data)
    my_log.logger.info("unzip file")
    # unzip del file
    shutil.unpack_archive(source_data_file, fld_data, 'zip')
    my_log.logger.info("file unzipped")


spark = SparkSession.builder.master("local").appName("London Bike Analysis").getOrCreate()


#conf = pyspark.SparkConf().setMaster("local[2]").setAppName("London Bike Analysis").setAll([("spark.driver.memory", "40g"), ("spark.executor.memory", "50g")])
#sc = SparkContext.getOrCreate(conf=conf)
#sqlC = SQLContext(sc)

df_londonBike_cl = ExectuteDfManage.manage_londonBike(londonBike, spark, my_log.logger)

my_log.logger.info ("-- SPARK QUERIRS JOIN GEODATA --")

# Create Buffer from Station Point
gdf_london_pois_200m = Buffer_sp.buffer_sp(londonStation, zip_london_pois, my_log.logger, radius, data_subfoler)
my_log.logger.info (" Spatial Join exectuted")

# Create df geo data --non passo direttamente da spark vrso pandas perchè è molo lento e alcune volte va in ou of memory
sdf_london_pois_200m = spark.read.option("delimiter", ",").option("header", True).csv(data_subfoler + "gdf_london_pois_200m.csv")
sdf_london_pois_200m.printSchema()
my_log.logger.info ("Spark dataframe London Point Interest close to station created")

# Inspection data
#sdf_london_pois_200mgp = sdf_london_pois_200m.groupBy(col('station_id'), col('fclass')).count()
#sdf_london_pois_200m.select(col("fclass")).distinct().show()

# Creation list for categorize london point of interest
list_values_shops =["clothes", "stationery", "beverages", "stationery", "department_store", "toy_shop", "video_shop", "beauty_shop", "sports_shop", "bicycle_shop", "furniture_shop", "gift_shop", "computer_shop", "bookshop", "shoe_shop", "outdoor_shop"]
list_values_bar_and_restaurants = ["butcher", "greengrocer", "bakery", "food_court", "restaurant", "bar", "cafe", "kiosk", "fast_food"]
list_values_health_and_beauty = ["veterinary", "hairdresser", "optician", "pharmacy", "hospital", "doctors", "nursing_home", "clinic", "community_centre"]
list_values_culture = ["arts_centre", "archaeological", "museum", "monument", "artwork", "theatre", "memorial"]
list_values_services = ["post_box", "laundry", "waste_basket", "shelter", "drinking_water", "vending_machine", "recycling_glass", "atm", "bicycle_rental", "recycling_clothes", "water_well", "newsagent", "car_wash", "vending_any", "post_office", "recycling", "recycling_paper", "hostel", "guesthouse", "wayside_shrine", "toilet", "car_rental", "telephone", "tourist_info", "vending_machine"]
list_values_public_areas= ["viewpoint", "bench", "park", "drinking_water", "market_place", "attraction", "fountain", "playground", "mall", "pitch", "dog_park", "picnic_site"]
list_values_education = ["college", "university", "school", "kindergarten", "stationery", "library"]
list_values_purchases = ["florist", "hotel", "car_dealership", "travel_agent", "jeweller", "guesthouse"]
list_values_leisure = ["nightclub", "cinema", "pub"]
list_values_sport = ["swimming_pool", "sports_centre"]
my_log.logger.info (" Categorize lists created")

# Assign category
sdf_london_pois_200m = sdf_london_pois_200m.withColumn("building_category",
                                        when(sdf_london_pois_200m["fclass"].isin(list_values_shops), "shop").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_bar_and_restaurants), "bar and restaurants").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_health_and_beauty), "helth and beauty").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_culture), "culture").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_services), "service").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_public_areas), "public area").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_education), "education").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_purchases), "purchase").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_leisure), "leisure").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_sport), "sport").\
                                          otherwise("Other"))

#sdf_london_pois_200m.select(col("building_category")).distinct().show()



# Calculate mode for Start and and Time
mode_start_time = df_londonBike_cl.groupby("start_station_id").agg(mode("start_time_of_day"))
mode_start_time = mode_start_time.withColumnRenamed('mode(start_time_of_day)', 'mode_start_time_of_day')

mode_end_time = df_londonBike_cl.groupby("end_station_id").agg(mode("end_time_of_day"))
mode_end_time = mode_end_time.withColumnRenamed('mode(end_time_of_day)', 'mode_end_time_of_day')

mode_start_time_pd = mode_start_time.toPandas()
mode_end_time_pd = mode_end_time.toPandas()
my_log.logger.info ("Mode created")

# Join Data of mode of start and time with geodata station point xy
gpd_london_station_xy = ExectuteGDfManage.gdf_london_station(londonStation, my_log.logger)

gpd_london_stat_xy_j_time_start = gpd_london_station_xy.merge(mode_start_time_pd, how='inner', left_on='station_id', right_on='start_station_id')
gpd_london_stat_xy_j_time_end = gpd_london_station_xy.merge(mode_end_time_pd, how='inner', left_on='station_id', right_on='end_station_id')

gpd_london_stat_xy_j_time_start.to_csv(data_subfoler + "to_plt_london_station_xy_j_start_time.csv")
gpd_london_stat_xy_j_time_end.to_csv(data_subfoler + "to_plt_london_station_xy_j_end_time.csv")

# Point of interest based on tyme of day report
pivot_id_station_pois = sdf_london_pois_200m.groupBy("station_id").pivot("building_category").count()
# Join Data of mode of start and time with geodata station point of interest
join_geo_point_int_start = pivot_id_station_pois.join(mode_start_time, pivot_id_station_pois.station_id == mode_start_time.start_station_id,"left")
join_geo_point_int_end = pivot_id_station_pois.join(mode_end_time, pivot_id_station_pois.station_id == mode_end_time.end_station_id,"left")

join_geo_point_int_start_pd = join_geo_point_int_start.toPandas()
join_geo_point_int_end_pd = join_geo_point_int_end.toPandas()

join_geo_point_int_start_pd.to_csv(data_subfoler + "to_plt_join_geo_point_int_start.csv")
join_geo_point_int_end_pd.to_csv(data_subfoler + "to_plt_join_geo_point_int_end.csv")
#df_londonBike_cl = spark.sparkContext.parallelize(range(0,20))
#print("From local[5] : "+str(df_londonBike_cl.getNumPartitions()))

# Use parallelize with 6 partitions
#df_londonBike_cl = spark.sparkContext.parallelize(range(0,25), 6)
#print("parallelize : "+str(df_londonBike_cl.getNumPartitions()))
#df_londonBike_cl = df_londonBike_cl.coalesce(10)

df_londonBike_cl = df_londonBike_cl.repartition(70)


my_log.logger.info ("-- SPARK QUERIES --")


# crete view
df_londonBike_cl.createOrReplaceTempView("VIEW_df_londonBike_cl")
my_log.logger.info("Created temporary VIEW_df_londonBike_cl")


# query 1 - TOP 30 link most frequently used
top30_link_cnt = spark.sql("SELECT COUNT(*) AS link_cnt, CONCAT(start_station_id,  '-' , end_station_id) AS link "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_station_id, end_station_id "
                          "ORDER BY link_cnt DESC LIMIT 30")
top30_link_cnt_pd = top30_link_cnt.toPandas()
top30_link_cnt_pd.to_csv(data_subfoler + "to_plt_top30_link_cnt.csv")
my_log.logger.info("created csv top30_link_cnt for plot created")

## query 2 - TOP 30 start station most frequently used
top30_start_st_cnt = spark.sql("SELECT COUNT(*) AS start_station_cnt, start_station_name "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_station_name "
                          "ORDER BY start_station_cnt DESC LIMIT 30")
top30_start_st_cnt_pd = top30_start_st_cnt.toPandas()
top30_start_st_cnt_pd.to_csv(data_subfoler + "to_plt_top30_start_st_cnt.csv")
my_log.logger.info("csv top30_start_st_cnt_pd for plot created")

## query 3 - TOP 30 end start station most frequently used
top30_end_st_cnt = spark.sql("SELECT COUNT(*) AS end_station_cnt, end_station_name "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY end_station_name "
                          "ORDER BY end_station_cnt DESC LIMIT 30")
top30_end_st_cnt_pd = top30_end_st_cnt.toPandas()
top30_end_st_cnt_pd.to_csv(data_subfoler + "to_plt_top30_end_st_cnt.csv")
my_log.logger.info("csv top30_end_st_cnt_pd for plot created")


## query 4 - Start Time of day frq
st_start_time_frq = df_londonBike_cl.groupBy("start_time_of_day").count().withColumn("frequency_time", col('count') / df_londonBike_cl.count()).orderBy(desc('frequency_time'))
st_start_time_frq_pd = st_start_time_frq.toPandas()
st_start_time_frq_pd.to_csv(data_subfoler + "to_plt_st_start_time_frq.csv")
my_log.logger.info("csv st_start_time_frq for plot created")

## query 5 - End Time of day frq
st_end_time_frq = df_londonBike_cl.groupBy("end_time_of_day").count().withColumn("frequency_time", col('count') / df_londonBike_cl.count()).orderBy(desc('frequency_time'))
st_end_time_frq_pd = st_end_time_frq.toPandas()
st_end_time_frq_pd.to_csv(data_subfoler + "to_plt_st_end_time_frq.csv")
my_log.logger.info("csv st_end_time_frq for plot created")

## query 6 - Day Week Frequency
st_day_week_frq = df_londonBike_cl.groupBy("start_day_of_week").count().withColumn('frequency_day', col("count") / df_londonBike_cl.count()).orderBy(desc('frequency_day'))
st_day_week_frq_pd = st_day_week_frq.toPandas()
st_day_week_frq_pd.to_csv(data_subfoler + "to_plt_st_day_week_frq.csv")
my_log.logger.info("csv st_day_week_frq_pd for plot created")

## query 7 - Month  Frequency
st_month_frq = df_londonBike_cl.groupBy("start_month").count().withColumn('frequency_month', col("count") / df_londonBike_cl.count()).orderBy(desc('frequency_month'))
st_month_frq_pd = st_month_frq.toPandas()
st_month_frq_pd.to_csv(data_subfoler + "to_plt_st_month_frq.csv")
my_log.logger.info("csv st_month_frq for plot created")

## query 8 - Date cnt
date_cnt = spark.sql("SELECT COUNT(*) AS start_day_cnt,  start_day "
                          "FROM VIEW_df_londonBike_cl "
                          "GROUP BY start_day "
                        "ORDER BY start_day")
date_cnt_pd = date_cnt.toPandas()
date_cnt_pd.to_csv(data_subfoler + "to_plt_date_cnt.csv")
my_log.logger.info("csv date_cnt for plot created")

## query 9 - Mean duration for each month
mean_month_duration = df_londonBike_cl.groupBy("start_month").avg("duration_hour").withColumnRenamed('avg(duration_hour)', 'mean_month_duration_hour').orderBy(desc('mean_month_duration_hour'))
mean_month_duration_pd = mean_month_duration.toPandas()
mean_month_duration_pd.to_csv(data_subfoler + "to_plt_mean_month_duration.csv")
my_log.logger.info("csv mean_month_duration for plot created")




my_log.logger.info ("-- SPARK QUERIRS JOIN GEODATA --")

# Create Buffer from Station Point
gdf_london_pois_200m = Buffer_sp.buffer_sp(londonStation, zip_london_pois, my_log.logger, radius, data_subfoler)
my_log.logger.info (" Spatial Join exectuted")

# Create df geo data --non passo direttamente da spark vrso pandas perchè è molo lento e alcune volte va in ou of memory
sdf_london_pois_200m = spark.read.option("delimiter", ",").option("header", True).csv(data_subfoler + "gdf_london_pois_200m.csv")
sdf_london_pois_200m.printSchema()
my_log.logger.info ("Spark dataframe London Point Interest close to station created")

# Inspection data
#sdf_london_pois_200mgp = sdf_london_pois_200m.groupBy(col('station_id'), col('fclass')).count()
#sdf_london_pois_200m.select(col("fclass")).distinct().show()

# Creation list for categorize london point of interest
list_values_shops =["clothes", "stationery", "beverages", "stationery", "department_store", "toy_shop", "video_shop", "beauty_shop", "sports_shop", "bicycle_shop", "furniture_shop", "gift_shop", "computer_shop", "bookshop", "shoe_shop", "outdoor_shop"]
list_values_bar_and_restaurants = ["butcher", "greengrocer", "bakery", "food_court", "restaurant", "bar", "cafe", "kiosk", "fast_food"]
list_values_health_and_beauty = ["veterinary", "hairdresser", "optician", "pharmacy", "hospital", "doctors", "nursing_home", "clinic", "community_centre"]
list_values_culture = ["arts_centre", "archaeological", "museum", "monument", "artwork", "theatre", "memorial"]
list_values_services = ["post_box", "laundry", "waste_basket", "shelter", "drinking_water", "vending_machine", "recycling_glass", "atm", "bicycle_rental", "recycling_clothes", "water_well", "newsagent", "car_wash", "vending_any", "post_office", "recycling", "recycling_paper", "hostel", "guesthouse", "wayside_shrine", "toilet", "car_rental", "telephone", "tourist_info", "vending_machine"]
list_values_public_areas= ["viewpoint", "bench", "park", "drinking_water", "market_place", "attraction", "fountain", "playground", "mall", "pitch", "dog_park", "picnic_site"]
list_values_education = ["college", "university", "school", "kindergarten", "stationery", "library"]
list_values_purchases = ["florist", "hotel", "car_dealership", "travel_agent", "jeweller", "guesthouse"]
list_values_leisure = ["nightclub", "cinema", "pub"]
list_values_sport = ["swimming_pool", "sports_centre"]
my_log.logger.info (" Categorize lists created")

# Assign category
sdf_london_pois_200m = sdf_london_pois_200m.withColumn("building_category",
                                        when(sdf_london_pois_200m["fclass"].isin(list_values_shops), "shop").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_bar_and_restaurants), "bar and restaurants").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_health_and_beauty), "helth and beauty").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_culture), "culture").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_services), "service").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_public_areas), "public area").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_education), "education").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_purchases), "purchase").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_leisure), "leisure").\
                                          when(sdf_london_pois_200m["fclass"].isin(list_values_sport), "sport").\
                                          otherwise("Other"))

#sdf_london_pois_200m.select(col("building_category")).distinct().show()



# Calculate mode for Start and and Time
mode_start_time = df_londonBike_cl.groupby("start_station_id").agg(mode("start_time_of_day"))
mode_start_time = mode_start_time.withColumnRenamed('mode(start_time_of_day)', 'mode_start_time_of_day')

mode_end_time = df_londonBike_cl.groupby("end_station_id").agg(mode("end_time_of_day"))
mode_end_time = mode_end_time.withColumnRenamed('mode(end_time_of_day)', 'mode_end_time_of_day')

mode_start_time_pd = mode_start_time.toPandas()
mode_end_time_pd = mode_end_time.toPandas()
my_log.logger.info ("Mode created")

# Join Data of mode of start and time with geodata station point xy
gpd_london_station_xy = ExectuteGDfManage.gdf_london_station(londonStation, my_log.logger)

gpd_london_stat_xy_j_time_start = gpd_london_station_xy.merge(mode_start_time_pd, how='inner', left_on='station_id', right_on='start_station_id')
gpd_london_stat_xy_j_time_end = gpd_london_station_xy.merge(mode_end_time_pd, how='inner', left_on='station_id', right_on='end_station_id')

gpd_london_stat_xy_j_time_start.to_csv(data_subfoler + "to_plt_london_station_xy_j_start_time.csv")
gpd_london_stat_xy_j_time_end.to_csv(data_subfoler + "to_plt_london_station_xy_j_end_time.csv")

# Point of interest based on tyme of day report
pivot_id_station_pois = sdf_london_pois_200m.groupBy("station_id").pivot("building_category").count()
# Join Data of mode of start and time with geodata station point of interest
join_geo_point_int_start = pivot_id_station_pois.join(mode_start_time, pivot_id_station_pois.station_id == mode_start_time.start_station_id,"left")
join_geo_point_int_end = pivot_id_station_pois.join(mode_end_time, pivot_id_station_pois.station_id == mode_end_time.end_station_id,"left")

join_geo_point_int_start_pd = join_geo_point_int_start.toPandas()
join_geo_point_int_end_pd = join_geo_point_int_end.toPandas()

join_geo_point_int_start_pd.to_csv(data_subfoler + "to_plt_join_geo_point_int_start.csv")
join_geo_point_int_end_pd.to_csv(data_subfoler + "to_plt_join_geo_point_int_end.csv")

spark.sparkContext.stop()