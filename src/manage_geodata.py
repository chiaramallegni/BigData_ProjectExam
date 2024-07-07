#import librerie esterne

import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# import classi interne
from variables_and_path import *

#lettura delle bike station csv
df_london_stations= pd.read_csv(londonStation)
print(df_london_stations.info())

# verifico se ci sono valori nulli sulle colonne
print(df_london_stations.loc[:, df_london_stations.isnull().any()].columns)

# verifico se ci sono valori npn univoci

station_id_exist = df_london_stations.station_id.is_unique
station_name_dup_exist = df_london_stations.station_name.is_unique
longitude_dup_exist = df_london_stations.longitude.is_unique
latitude_dup_exist = df_london_stations.latitude.is_unique

# creo il data frame delle coordinate duplicate... per ora non li elimino.
if (longitude_dup_exist == False) and (latitude_dup_exist == False):
    station_pointgeo_duplicate = df_london_stations.loc[df_london_stations.duplicated(subset=['longitude', 'latitude'])]
    print(station_pointgeo_duplicate.head(20))

# elimino eventuali id duplicti
df_london_stations = df_london_stations.drop_duplicates(subset=['station_id'], keep='first')

# creazione geometria puntuale delle bike station
geometry=gpd.points_from_xy(df_london_stations['longitude'],df_london_stations['latitude'])
gdf_london_stations = gpd.GeoDataFrame(df_london_stations, crs='EPSG:4326', geometry=geometry)
print(gdf_london_stations.info())

# lettura shp file dei building di londra
print('Sto leggendo lo shp file dei buildings di londra')
gdf_london_buildings = gpd.read_file(zip_london_buildings)
print('Shp buildings di londra letto:  ')
print(gdf_london_buildings.info())

# lettura shp file dei punti di interessa di londra
print('Sto leggendo lo shp file dei punti di interesse di londra')
gdf_london_pois = gpd.read_file(zip_london_pois)
print('Shp punti di interesse di londra letto:  ')
print(gdf_london_pois.info())

# lettura shp file dei punti fermate del trasporto pubblico di londra
print('Sto leggendo lo shp file dei punti fermate del trasporto pubblico di londra')
gdf_london_railway_station = gpd.read_file(zip_london_railway_station)
print('Shp punti fermate del trasporto pubblico di londra letto:  ')
print(gdf_london_railway_station.info())

# lettura shp file delle linee del trasporto pubblico di londra
print('Sto leggendo lo shp  delle linee del trasporto pubblico di londra')
gdf_london_railway = gpd.read_file(zip_london_railway)
print('Shp linee del trasporto pubblico di londra letto:  ')
print(gdf_london_railway.info())

# plotto e salvo l'immagine delle bike stations
fig, bike_stations = plt.subplots(1, 1)
fig.set_size_inches(15,7)
gdf_london_stations.plot(ax=bike_stations, color='red', alpha=0.5, marker='+')
gdf_london_buildings.plot(ax=bike_stations, color='blue', alpha=1, marker='o')

minx, miny, maxx, maxy = gdf_london_buildings.total_bounds
bike_stations.set_xlim(minx, maxx)
bike_stations.set_ylim(miny, maxy)

legend_elements = [
    plt.plot([],[], color='red', alpha=0.5, marker='+', label='Bike stations', ls='')[0],
    plt.plot([],[], color='blue', alpha=1, marker='o', label='London buildings', ls='')[0]]
bike_stations.legend(handles=legend_elements, loc='upper right')
bike_stations.get_xaxis().set_visible(False)
bike_stations.get_yaxis().set_visible(False)
plt.savefig(fld_image + '/bike_stations.png')
#plt.show()


# proettto i dati geografici : modifico sistema di riferimento da geografico e proiettato in mod da persormare meglio sulle query spaziali (costruizone buffer, vicinanza etc)

gdf_london_stations_prj = gdf_london_stations.to_crs('EPSG:27700')
gdf_london_buildings_prj = gdf_london_buildings.to_crs('EPSG:27700')
gdf_london_pois_prj = gdf_london_pois.to_crs('EPSG:27700')
gdf_london_railway_station_prj = gdf_london_railway_station.to_crs('EPSG:27700')
gdf_london_railway_prj = gdf_london_railway.to_crs('EPSG:27700')


# # creo una copia del gdf delle stazioni e sostituisco  ma sostituisco la geometria cpountuale con una poligonale: buffer di 200 m intorno alle stazioni
gdf_london_buffer_stations = gdf_london_stations_prj.copy()
gdf_london_buffer_stations['geometry'] = gdf_london_stations_prj.buffer(
    radius, resolution=5, cap_style=1, join_style=1, mitre_limit=2)
print(gdf_london_buffer_stations.info())

# plotto e salvo immagine dei buffer station
fig, buffer_station = plt.subplots(1, 1)
fig.set_size_inches(15,7)
gdf_london_stations_prj.plot(ax=buffer_station, color='red', alpha=0.5, marker='+')
gdf_london_buildings_prj.plot(ax=buffer_station, color='blue', alpha=1, marker='o')
gdf_london_buffer_stations.plot(ax=buffer_station, facecolor='none', edgecolor='green',  alpha=0.5, marker='o')

minx, miny, maxx, maxy = gdf_london_buildings_prj.total_bounds
buffer_station.set_xlim(minx, maxx)
buffer_station.set_ylim(miny, maxy)

legend_elements = [
    plt.plot([],[], color='red', alpha=0.5, marker='+', label=' Bike stations', ls='')[0],
    plt.plot([],[], color='blue', alpha=1, marker='o', label='London buildings', ls='')[0],
    plt.plot([],[], color='green', alpha=0.5, marker='o', label='Bike stations Buffers 200 m', ls='')[0]]

buffer_station.legend(handles=legend_elements, loc='upper right')
buffer_station.get_xaxis().set_visible(False)
buffer_station.get_yaxis().set_visible(False)
plt.savefig(fld_image + '/buffer_station.png')
#plt.show()


# eseguo una spatial join per intercettare tutti i building di londra che si trovano in un raggio di 200 metri dalle bike station e creo un df dedicato

gfd_buildings_200m = gpd.sjoin(gdf_london_buildings_prj, gdf_london_buffer_stations, how='inner', predicate='intersects')
print(gfd_buildings_200m.groupby(['station_id'])['station_id'].value_counts())

# eseguo una spatial join per intercettare tutti i punti di interesse di londra che si trovano in un raggio di 200 metri dalle bike station e creo un df dedicato

gdf_london_pois_200m = gpd.sjoin(gdf_london_pois_prj, gdf_london_buffer_stations, how='inner', predicate='intersects')
print(gdf_london_pois_200m.groupby(['station_id'])['station_id'].value_counts())

# eseguo una spatial join per intercettare tutte le fermate treno e metro di londra che si trovano in un raggio di 200 metri dalle bike station e creo un df dedicato

gdf_london_railway_station_200m = gpd.sjoin(gdf_london_railway_station_prj, gdf_london_buffer_stations, how='inner', predicate='intersects')
print(gdf_london_railway_station_200m.groupby(['station_id'])['station_id'].value_counts())

# modifico il fomato dle campo geometri per i gfd creati in modo che possano essere letti da spark






# In[ ]:




