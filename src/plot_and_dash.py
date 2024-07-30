import pandas as pd
from dash import Dash, html, dcc, Input, Output
import plotly.express as px

from plots import Plotting
from set_log import MyLogger
from variables_and_path import *

Mylog = MyLogger()

Mylog.logger.info ("-- CREATING PANDAS DATAFRAME  --")

top30_link_cnt = pd.read_csv(data_subfoler + 'to_plt_top30_link_cnt.csv')
top30_start_st_cnt = pd.read_csv(data_subfoler + 'to_plt_top30_start_st_cnt.csv')
top30_end_st_cnt = pd.read_csv(data_subfoler + 'to_plt_top30_end_st_cnt.csv')
st_start_time_frq = pd.read_csv(data_subfoler + 'to_plt_st_start_time_frq.csv')
st_end_time_frq = pd.read_csv(data_subfoler + 'to_plt_st_end_time_frq.csv')
st_day_week_frq = pd.read_csv(data_subfoler + 'to_plt_st_day_week_frq.csv')
st_month_frq = pd.read_csv(data_subfoler + 'to_plt_st_month_frq.csv')
date_cnt = pd.read_csv(data_subfoler + 'to_plt_date_cnt.csv')
mean_month_duration = pd.read_csv(data_subfoler + 'to_plt_mean_month_duration.csv')
gpd_london_stat_xy_j_time_start = pd.read_csv(data_subfoler + 'to_plt_london_station_xy_j_start_time.csv')
gpd_london_stat_xy_j_time_end= pd.read_csv(data_subfoler + 'to_plt_london_station_xy_j_end_time.csv')
sdf_london_pois_200m_cnt= pd.read_csv(data_subfoler + 'to_plt_london_pois_Station_sizing.csv')
df_londonBike_start_time_day_cnt= pd.read_csv(data_subfoler + 'to_plt_start_time_of_day_station_sizing.csv')
df_londonBike_end_time_day_cnt= pd.read_csv(data_subfoler + 'to_plt_end_time_of_day_station_sizing.csv')


Mylog.logger.info ("-- POLTS --")

color_scale_day = {"morning" : "red", "afthernoon" : "blue", "night" : "green", "dinner": "orange", "lunch" : "yellow"}

## plot 1 - TOP 30 start station most frequently used
PL_bar_top30_link_cnt =  Plotting.bar_plot_blured(top30_link_cnt, 'link', 'link_cnt', 'TOP 30 Link Station Count', fld_image, '/top30_link_count.html')
Mylog.logger.info("Plotted TOP 30 link count")
## plot 2 - TOP 30 start station most frequently used
PL_bar_top30_start_st_cnt = Plotting.bar_plot_blured(top30_start_st_cnt, 'start_station_name', 'start_station_cnt', 'TOP 30 Start Station Count', fld_image, '/top30_start_st_count.html')
Mylog.logger.info("Plotted TOP 30 Start Station count")
## plot 3 - TOP 30 end start station most frequently used
PL_bar_top30_end_st_cnt = Plotting.bar_plot_blured(top30_end_st_cnt, 'end_station_name', 'end_station_cnt', 'TOP 30 End Station Count',  fld_image, '/top30_start_end_count.html')
Mylog.logger.info("Plotted TOP 30 End Station count")
## plot 4 -  - Day Week Frquency
PL_bar_day_week_frq = Plotting.bar_plot_blured(st_day_week_frq, 'start_day_of_week', 'frequency_day', 'Start Day of Week Frequency', fld_image, '/st_day_week_frq.html')
Mylog.logger.info("Plotted Start Day Week Frequency")
## plot 5 - Month Frquency
PL_bar_st_month_frq = Plotting.bar_plot_blured(st_month_frq, 'start_month', 'frequency_month', 'Start Month Frequency', fld_image, '/st_month_frq.html')
Mylog.logger.info("Plotted Start Month Frequency")
## plot 6 - Start Time if Day Frquency
PL_bar_st_start_time_frq = Plotting.bar_plot_blured(st_start_time_frq, 'start_time_of_day', 'frequency_time', 'Start Time of day Frequency', fld_image, '/st_start_time_of_day_frq.html')
Mylog.logger.info("Plotted Start Time of Day Frequency")
## plot 7 - End Time if Day Frquency
PL_bar_st_end_time_frq = Plotting.bar_plot_blured(st_end_time_frq, 'end_time_of_day', 'frequency_time', 'End Time of day Frequency', fld_image, '/st_end_time_of_day_frq.html')
Mylog.logger.info("Plotted Start Time of Day Frequency")
## plot 8 - Mean duration for each month
PL_bar_mean_month_duration = Plotting.bar_plot_blured(mean_month_duration, 'start_month', 'mean_month_duration_hour', 'Mean Duration for Month', fld_image, '/mean_month_duration.html')
Mylog.logger.info("Ploted Count Day Runs Over Time")
## plot 9 - Date cnt
PL_line_date_cnt = Plotting.line_plot_time(date_cnt, 'start_day', 'start_day_cnt', 'Count Day Runs Over Time', fld_image, '/cnt_day_runs_over_time.html')
Mylog.logger.info("Ploted Count Day Runs Over Time")


## plot 10 - Map for start time of day
PL_map_station_day_start = px.scatter_mapbox(gpd_london_stat_xy_j_time_start,
                             lat="latitude", lon="longitude", hover_name="station_name",
                             color = "mode_start_time_of_day",  color_discrete_map=color_scale_day, text='station_name',
                             zoom=10, width=1000
                             )
PL_map_station_day_start.update_layout(mapbox_style="carto-positron")
PL_map_station_day_start.update_layout(autosize=True, hovermode='closest', title = 'London Bike Station Day Time at Start',
                           mapbox=dict(bearing=0, pitch=0), margin={"r": 0, "t": 0, "l": 0, "b": 0})
PL_map_station_day_start.write_html(fld_image + '/map_station_day_start.html')
Mylog.logger.info("London Bike Station Day Time at Start")

## plot 11 - Map for end time of day
PL_map_station_day_end = px.scatter_mapbox(gpd_london_stat_xy_j_time_end,
                             lat="latitude", lon="longitude", hover_name="station_name",
                             color = "mode_end_time_of_day", color_discrete_map=color_scale_day, text='station_name',
                             zoom=10, width=1000
                             )
PL_map_station_day_end.update_layout(mapbox_style="carto-positron")
PL_map_station_day_end.update_layout(autosize=True, hovermode='closest', title = 'London Bike Station Day Time at End',
                           mapbox=dict(bearing=0, pitch=0), margin={"r": 0, "t": 0, "l": 0, "b": 0})
PL_map_station_day_end.write_html(fld_image + '/map_station_day_end.html')
Mylog.logger.info("London Bike Station Day Time at End")

## plot 12 - Map for Station based on start time of day
PL_map_londonBike_start_time_day_cnt = px.scatter_mapbox(df_londonBike_start_time_day_cnt,
                             lat="latitude", lon="longitude", hover_name="station_name",
                             color = "start_time_of_day", color_discrete_map=color_scale_day, text='station_name',
                             zoom=10, width=1000, size = "count"
                             )
PL_map_londonBike_start_time_day_cnt.update_layout(mapbox_style="carto-positron")
PL_map_londonBike_start_time_day_cnt.update_layout(autosize=True, hovermode='closest', title = 'Station Sizing based on Start Time of Day',
                           mapbox=dict(bearing=0, pitch=0), margin={"r": 0, "t": 0, "l": 0, "b": 0})
PL_map_londonBike_start_time_day_cnt.write_html(fld_image + '/PL_map_start_time_of_day_station_sizing.html')
Mylog.logger.info("Station Sizing based on Start Time of Day")

## plot 13 - Map for Station bsize based on end time of day
PL_map_londonBike_end_time_day_cnt = px.scatter_mapbox(df_londonBike_end_time_day_cnt,
                             lat="latitude", lon="longitude", hover_name="station_name",
                             color = "end_time_of_day", color_discrete_map=color_scale_day, text='station_name',
                             zoom=10, width=1000, size = "count"
                             )
PL_map_londonBike_end_time_day_cnt.update_layout(mapbox_style="carto-positron")
PL_map_londonBike_end_time_day_cnt.update_layout(autosize=True, hovermode='closest', title = 'Station Sizing based on End Time of Day',
                           mapbox=dict(bearing=0, pitch=0), margin={"r": 0, "t": 0, "l": 0, "b": 0})
PL_map_londonBike_end_time_day_cnt.write_html(fld_image + '/PL_map_end_time_of_day_station_sizing.html')
Mylog.logger.info("Station Sizing based on End Time of Day")

## plot 14 - Map for Station bsize based on point of interest
PL_map_london_pois_200m_cnt = px.scatter_mapbox(sdf_london_pois_200m_cnt,
                             lat="latitude", lon="longitude", hover_name="station_name_x",
                             color = "building_category",  text='station_name_x',
                             zoom=10, width=1000, size = "count"
                             )
PL_map_london_pois_200m_cnt.update_layout(mapbox_style="carto-positron")
PL_map_london_pois_200m_cnt.update_layout(autosize=True, hovermode='closest', title = 'Station Sizing based london point of interest',
                           mapbox=dict(bearing=0, pitch=0), margin={"r": 0, "t": 0, "l": 0, "b": 0})
PL_map_london_pois_200m_cnt.write_html(fld_image + '/PL_map_london_pois_Station_sizing.html')
Mylog.logger.info("Station Sizing based london point of interest")



Mylog.logger.info ("-- DASHBOARD --")

app = Dash()
color = {'background': '#111111', 'text': '#7FDBFF'}

app.layout = html.Div(children=[dcc.Store(id='fig_store'),
    html.H1(children='London Bike Analysis', style ={'textAlign': 'center', 'color': color['text']}), #titolo
    html.Div('London Bike plots',  style ={'textAlign': 'center', 'color': color['text']}),
    html.Table(
        [
        html.Tr([
            html.Td(dcc.Graph(id='plot 1', figure= PL_bar_top30_link_cnt,
                              style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'middle',
                                     'margin-left': '25vw', 'margin-top': '3vw'}),colSpan= 2)
        ]),
        html.Tr([
            html.Td(dcc.Graph(id='plot 2', figure=PL_bar_top30_start_st_cnt,
                              style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'middle',
                                     'margin-left': '3vw', 'margin-top': '3vw'})),
            html.Td(dcc.Graph(id='plot 3', figure=PL_bar_top30_end_st_cnt,
                              style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'middle',
                                     'margin-left': '1vw', 'margin-top': '3vw'}))
        ]),
        html.Tr([
            html.Td(dcc.Graph(id='plot 4', figure=PL_bar_day_week_frq,
                              style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'middle',
                                     'margin-left': '3vw', 'margin-top': '3vw'})),
            html.Td(dcc.Graph(id='plot 5', figure=PL_bar_st_month_frq,
                              style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'middle',
                                     'margin-left': '1vw', 'margin-top': '3vw'}))
        ]),
        html.Tr([
            html.Td(dcc.Graph(id='plot 6', figure=PL_bar_st_start_time_frq,
                                style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'middle',
                                         'margin-left': '3vw', 'margin-top': '3vw'})),
            html.Td(dcc.Graph(id='plot 7', figure=PL_bar_st_end_time_frq,
                                style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'middle',
                                         'margin-left': '1vw', 'margin-top': '3vw'}))
        ]),
        html.Tr([
            html.Td(dcc.Graph(id='plot 8', figure= PL_bar_mean_month_duration,
                              style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'middle',
                                     'margin-left': '3vw', 'margin-top': '3vw'})),
            html.Td(dcc.Graph(id='plot 9', figure=PL_line_date_cnt,
                              style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'middle',
                                     'margin-left': '1vw', 'margin-top': '3vw'}))
        ]),
            html.Tr([
                html.Td(dcc.Graph(id='plot 10', figure=PL_map_station_day_start,
                                  style={'width': '70%', 'display': 'inline-block', 'vertical-align': 'middle',
                                         'margin-left': '3vw', 'margin-top': '3vw'})),
                html.Td(dcc.Graph(id='plot 11', figure=PL_map_station_day_end,
                                  style={'width': '70%', 'display': 'inline-block', 'vertical-align': 'middle',
                                         'margin-left': '1vw', 'margin-top': '3vw'}))
            ]),
            html.Tr([
                html.Td(dcc.Graph(id='plot 12', figure=PL_map_londonBike_start_time_day_cnt,
                                  style={'width': '70%', 'display': 'inline-block', 'vertical-align': 'middle',
                                         'margin-left': '3vw', 'margin-top': '3vw'})),
                html.Td(dcc.Graph(id='plot 13', figure=PL_map_londonBike_end_time_day_cnt,
                                  style={'width': '70%', 'display': 'inline-block', 'vertical-align': 'middle',
                                         'margin-left': '1vw', 'margin-top': '3vw'}))
            ]),
            html.Tr([
                html.Td(dcc.Graph(id='plot 14', figure=PL_map_london_pois_200m_cnt,
                                  style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'middle',
                                         'margin-left': '25vw', 'margin-top': '3vw'}), colSpan=2)
            ]),

])])


# Run local server
if __name__ == '__main__':
    app.run_server(debug=True)