from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from graphframes import GraphFrame

from set_log import MyLogger
from df_cleaning_and_manage import ExectuteDfManage
from variables_and_path import *

my_log = MyLogger()

my_log.logger.info("-- START GRAPH FRAME --")

spark = SparkSession.builder.master("local").appName("London Bike Graph Frame").getOrCreate()

# GRAPHFRAME
df_londonBike_cl = ExectuteDfManage.manage_londonBike(londonBike, spark, my_log.logger)

df_edge = df_londonBike_cl.groupBy(col('start_station_id'), col('end_station_id')).count().alias('bike_run_count')
#df_edge  = df_edge.filter(col("start_station_id") != col("end_station_id"))

#assegna un nuovo nome alle colonne
df_edge = df_edge.withColumnRenamed('start_station_id', 'src')
df_edge = df_edge.withColumnRenamed('end_station_id', 'dst')
df_edge = df_edge.withColumnRenamed('count', 'weight')
df_edge.show()

src = df_edge.select('src')
dst = df_edge.select('dst')
df_node = src.union(dst).distinct().withColumnRenamed('src', 'id')
g_london_stations = GraphFrame(df_node, df_edge)


# Degree Analysis

g_london_stations.inDegrees.orderBy(desc('inDegree')).show(30)
g_london_stations.outDegrees.orderBy(desc('outDegree')).show(30)
g_london_stations.degrees.orderBy(desc('degree')).show(30)

#g_component = g_london_stations.stronglyConnectedComponents(maxIter=10).orderBy("component")
#g_component.select("id", "component").orderBy("component").show(30)
#g_component.groupBy("component")

#g_triang = g_london_stations.triangleCount()
#g_triang.select("id", "count").orderBy("count")
spark.sparkContext.stop()