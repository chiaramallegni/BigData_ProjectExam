
import geopandas as gpd
import pandas as pd

class DfUtilities:
    def __init__(self, gdf):
        self.gdf=gdf


# Spark prevede una colonna geospaziale come stringa WKT.
# Internamente lo utilizza per creare geometrie OGC tramite Java Topology Suite (JTS).
# Quindi, per utilizzare Spatial Spark aggiungeremo la colonna WKT ai nostri dati.

def geometry_to_wkt (gdf):
    gdf['wkt'] = pd.Series(
            map(lambda geom: geom.wkt, gdf['geometry']),
            index=gdf.index, dtype='string')
    gdf.drop("geometry", axis=1, inplace = True)
    return gdf
