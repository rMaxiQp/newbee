import numpy as np
import pandas as pd
import folium
from folium import plugins
from folium.plugins import HeatMap

# read a csv file into a dataframe
df_acc = pd.read_csv('Chicago_Crimes_2012_to_2017.csv',dtype=object)
df_acc['Latitude'] = df_acc['Latitude'].astype(float)
df_acc['Longitude'] = df_acc['Longitude'].astype(float)
my_map = folium.Map(location=[41.881832, -87.623177], zoom_start = 10)
heat_df = df_acc[['Latitude', 'Longitude']]
heat_df = heat_df.dropna(axis=0, subset=['Latitude','Longitude'])
heat_data = [[row['Latitude'],row['Longitude']] for index, row in heat_df.iterrows()]

# create heatmap based on the coordiates in the data frame
HeatMap(heat_data, radius = 5, blur = 10, max_val = 1, min_opacity = 0.5).add_to(my_map)
my_map.save('chicagoMKcluster.html')
