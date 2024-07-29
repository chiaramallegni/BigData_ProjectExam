from utilities import Utilities
import os
#mode for crete folder
mode_cf = 0o666

my_ut = Utilities()

# parent drirectory project
parent_dir = '../' # il . sta per la cartella sopra (. = 1 cartella sopra, .. 2 cartelle sopra)

# project folder
fld_data = my_ut.create_folder(parent_dir,'data',mode_cf)
fld_image = my_ut.create_folder(parent_dir,'exp_image',mode_cf)
fld_log = my_ut.create_folder(parent_dir,'log',mode_cf)


# data paths

## name subfolder post unzip data file
data_subfoler = fld_data + '/london_bike/'

# csv corse bike sharing
londonBike = data_subfoler + 'london.csv'
# csv coordinate stazioni bike sharing
londonStation = data_subfoler + 'london_stations.csv'
# zip shp file london building

zip_london_buildings = data_subfoler + 'gis_osm_buildings_a_free_1.zip'
# zip shp file london point of interest
zip_london_pois = data_subfoler + 'gis_osm_pois_free_1.zip'
# zip shp file london railways station
zip_london_railway_station = data_subfoler + 'gis_osm_transport_free_1_railway_station.zip'
# zip shp file london railways station
zip_london_railway = data_subfoler + 'gis_osm_railways_free_1.zip'

#raggio di buffer per le stazioni in metri
radius = 200