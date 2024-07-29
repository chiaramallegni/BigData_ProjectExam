import plotly.express as px
import matplotlib.pyplot as plt

class Plotting:
    @staticmethod
    def plot_bike_station(gdf_london_stations, gdf_london_buildings, fld_image):
        fig, bike_stations = plt.subplots(1, 1)
        fig.set_size_inches(15, 7)
        gdf_london_stations.plot(ax=bike_stations, color='red', alpha=0.5, marker='+')
        gdf_london_buildings.plot(ax=bike_stations, color='blue', alpha=1, marker='o')

        minx, miny, maxx, maxy = gdf_london_buildings.total_bounds
        bike_stations.set_xlim(minx, maxx)
        bike_stations.set_ylim(miny, maxy)

        legend_elements = [
            plt.plot([], [], color='red', alpha=0.5, marker='+', label='Bike stations', ls='')[0],
            plt.plot([], [], color='blue', alpha=1, marker='o', label='London buildings', ls='')[0]]
        bike_stations.legend(handles=legend_elements, loc='upper right')
        bike_stations.get_xaxis().set_visible(False)
        bike_stations.get_yaxis().set_visible(False)
        plt.savefig(fld_image + '/bike_stations.png')
        # plt.show()
    @staticmethod
    def plot_buffer_station(gdf_london_stations_prj, gdf_london_buildings_prj, gdf_london_buffer_stations, fld_image):
        fig, buffer_station = plt.subplots(1, 1)
        fig.set_size_inches(15, 7)
        gdf_london_stations_prj.plot(ax=buffer_station, color='red', alpha=0.5, marker='+')
        gdf_london_buildings_prj.plot(ax=buffer_station, color='blue', alpha=1, marker='o')
        gdf_london_buffer_stations.plot(ax=buffer_station, facecolor='none', edgecolor='green', alpha=0.5, marker='o')

        minx, miny, maxx, maxy = gdf_london_buildings_prj.total_bounds
        buffer_station.set_xlim(minx, maxx)
        buffer_station.set_ylim(miny, maxy)

        legend_elements = [
            plt.plot([], [], color='red', alpha=0.5, marker='+', label=' Bike stations', ls='')[0],
            plt.plot([], [], color='blue', alpha=1, marker='o', label='London buildings', ls='')[0],
            plt.plot([], [], color='green', alpha=0.5, marker='o', label='Bike stations Buffers 200 m', ls='')[0]]

        buffer_station.legend(handles=legend_elements, loc='upper right')
        buffer_station.get_xaxis().set_visible(False)
        buffer_station.get_yaxis().set_visible(False)
        plt.savefig(fld_image + '/buffer_station.png')
        # plt.show()
    @staticmethod
    def bar_plot_blured(df, x, y, title, fld_image, image_name):
        bar_plot=px.bar(df, x=x, y=y,
                    text_auto=True, labels=x,
                    title=title,
                    color=y,
                    color_continuous_scale='Bluered',
                    width=1000,
                    height=600)
        bar_plot.write_html(fld_image + image_name)
        return bar_plot
    @staticmethod
    def line_plot_time(df, x, y, title, fld_image, image_name):
        line_plot=px.line(df, x=x, y=y,
                    title=title,
                    width=1300,
                    height=600,
                    hover_data={"start_day": "|%B %d, %Y"})
        line_plot.update_traces(mode="markers",
                                       marker_size=7,
                                       marker_color="red"
                                       # line_color = "black")
                                       )
        line_plot.update_xaxes(dtick="M1",
                                      tickformat="%b\%Y",
                                      ticks="inside",
                                      ticklen=2)
        line_plot.write_html(fld_image + image_name)
        return line_plot

    def map_plot_simple_point(df, color, title, fld_image, image_name):
        map_plot = px.scatter_mapbox(df, lat = "latitude", lon = "longitude", hover_name ="station_name",
                                color=color, zoom=8.5, title=title, text = 'station_name')
        map_plot.update_layout(mapbox_style="carto-positron")
        map_plot.update_layout(autosize=True, hovermode='closest' ,
                          mapbox= dict(bearing=0,  pitch=0), margin={"r":0,"t":0,"l":0,"b":0})
        map_plot.write_html(fld_image + image_name)

