#import external libraries
import pandas as pd
import geopandas as gpd



class ExectuteGDfManage:
    @staticmethod
    def gdf_london_station(londonStation, logger):
        # reading csv
        df_london_stations = pd.read_csv(londonStation)
        print(df_london_stations.info())
        logger.info('Created : df london_stations')
        logger.info('dataframe head 5 - {}'.format(df_london_stations.head(5)))

        # verify null value
        logger.info('Valori null = {}'.format(df_london_stations.loc[:, df_london_stations.isnull().any()].columns))

        # verify unique value
        station_id_exist = df_london_stations.station_id.is_unique
        station_name_dup_exist = df_london_stations.station_name.is_unique
        longitude_dup_exist = df_london_stations.longitude.is_unique
        latitude_dup_exist = df_london_stations.latitude.is_unique

        # Create Dataframe from duplicate coordinates
        if (longitude_dup_exist == False) and (latitude_dup_exist == False):
            station_pointgeo_duplicate = df_london_stations.loc[
                df_london_stations.duplicated(subset=['longitude', 'latitude'])]
            print(station_pointgeo_duplicate.head(20))

        # Delete duplicate id_station
        df_london_stations = df_london_stations.drop_duplicates(subset=['station_id'], keep='first')
        logger.info('Deleted duplicate value in Station_id')

        # Created geometry point from xy
        geometry = gpd.points_from_xy(df_london_stations['longitude'], df_london_stations['latitude'])
        gdf_london_stations = gpd.GeoDataFrame(df_london_stations, crs='EPSG:4326', geometry=geometry)
        print(gdf_london_stations.info())
        logger.info('Created geometry point from xy')

        # Assign projection
        gdf_london_stations_prj = gdf_london_stations.to_crs('EPSG:27700')
        logger.info('London Station Projected')

        return gdf_london_stations_prj

    # Create Geodataframe london point of interest from shape file
    @staticmethod
    def gdf_point_interest(zip_london_pois, logger):
        # gdf from  zip
        gdf_london_pois = gpd.read_file(zip_london_pois)
        print(gdf_london_pois.info())
        logger.info('Created : gdf london_pois')

        # Assign projection
        gdf_london_pois_prj = gdf_london_pois.to_crs('EPSG:27700')
        logger.info('Geodata Projected')

        return gdf_london_pois_prj

    @staticmethod
    # Create Geodataframe london point of interest from shape file
    def gdf_building(zip_london_buildings, logger):
        # gdf from  zip
        gdf_london_buildings = gpd.read_file(zip_london_buildings)
        print(gdf_london_buildings.info())
        logger.info('Created : gdf london_buildings')

        # Assign projection
        gdf_london_buildings_prj = gdf_london_buildings.to_crs('EPSG:27700')
        logger.info('Geodata Projected')

        return gdf_london_buildings_prj




