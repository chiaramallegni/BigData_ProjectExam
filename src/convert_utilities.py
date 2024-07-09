
import geopandas as gpd
import pandas as pd
from graphframes import GraphFrame
from shapely.geometry import Point, Polygon, shape # creating geospatial data
from shapely import wkb, wkt # creating and parsing geospatial data




class DfUtilities:
    def __init__(self, gdf):
        self.gdf=gdf


# Spark prevede una colonna geospaziale come stringa WKT.
# Internamente lo utilizza per creare geometrie OGC tramite Java Topology Suite (JTS).
# Quindi, per utilizzare Spatial Spark aggiungeremo la colonna WKT ai nostri dati.

#def geometry_to_wkt (gdf):
    #gdf['wkt'] = pd.Series(
           # map(lambda geom: str(geom.to_wkt()), gdf['geometry']),
            #index=gdf.index, dtype='string')
    #gdf.drop("geometry", axis=1, inplace = True)
    #return gdf

from pyspark.sql.types import *

# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return DoubleType()
    elif f == 'float32': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types):
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)




def gen_graph_from_edgeList (slef, e):
    v = e \
        .select('src') \
        .union(e.select('dst')) \
        .distinct() \
        .withColumnRenamed('src', 'id')
    return GraphFrame(v,e)