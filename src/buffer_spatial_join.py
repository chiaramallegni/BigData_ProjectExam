import geopandas as gpd
from gdf_cleaning_and_manage import ExectuteGDfManage

class Buffer_sp:
    @staticmethod
    def buffer_sp(londonStation, zip_london_pois,   logger, radius, data_subfoler):
        gdf_london_stations_prj = ExectuteGDfManage.gdf_london_station(londonStation, logger)
        gdf_london_pois_prj = ExectuteGDfManage.gdf_point_interest(zip_london_pois, logger)
        # Create Buffer from Station Point
        gdf_london_buffer_stations = gdf_london_stations_prj.copy()
        gdf_london_buffer_stations['geometry'] = gdf_london_stations_prj.buffer(
            radius, resolution=5, cap_style=1, join_style=1, mitre_limit=2)
        print(gdf_london_buffer_stations.info())

        logger.info('Created Geodata Buiffer Station from station point')

        # Creat GeoDataFrame Spatial Join for point of interset close  200 from station id

        gdf_london_pois_200m = gpd.sjoin(gdf_london_pois_prj, gdf_london_buffer_stations, how='inner', predicate='intersects')

        logger.info('Created GeoDataFrame from Spatial Join')
        gdf_london_pois_200m.to_csv(data_subfoler + "gdf_london_pois_200m.csv")

        return gdf_london_pois_200m