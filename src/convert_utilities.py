import geopandas as gpd
import pandas as pd
from graphframes import GraphFrame
from shapely.geometry import Point, Polygon, shape # creating geospatial data
from shapely import wkb, wkt # creating and parsing geospatial data




class DfUtilities:
    def __init__(self, gdf, e):
        self.gdf=gdf
        self.e=e


# Spark prevede una colonna geospaziale come stringa WKT.
# Internamente lo utilizza per creare geometrie OGC tramite Java Topology Suite (JTS).
# Quindi, per utilizzare Spatial Spark aggiungeremo la colonna WKT ai nostri dati.

def geometry_to_wkt (gdf):
    gdf['wkt'] = pd.Series(
           map(lambda geom: str(geom.to_wkt()), gdf['geometry']),
            index=gdf.index, dtype='string')
    gdf.drop("geometry", axis=1, inplace = True)
    return gdf

def gen_graph_from_edgeList (slef, e):
    v = e \
        .select('src') \
        .union(e.select('dst')) \
        .distinct() \
        .withColumnRenamed('src', 'id')
    return GraphFrame(v,e)