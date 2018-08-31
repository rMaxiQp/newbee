import csv
import pandas as pd
import folium
from folium import plugins
from folium.plugins import HeatMap

# Daytime to be "am"
def selectDay(x):
    raw = x.split()
    if(len(raw) != 3):
        return False

    value = (int(raw[1][:2]) + 12) % 24
    if(raw[2] == "PM"):
        return True
    else:
        return False

# night time will be PM
def selectNight(x):
    raw = x.split()
    if(len(raw) != 3):
        return False

    if(raw[2] == "PM"):
        value = (int(raw[1][:2]) + 12) % 24
        return True
    else:
        return False
fd = open('Chicago_Crimes_2012_to_2017.csv', 'r')
my_map_day = folium.Map(location=[41.881832, -87.623177], zoom_start = 13)
my_map_night = folium.Map(location=[41.881832, -87.623177], zoom_start = 13)
# df_acc = pd.read_csv('Chicago_Crimes_2012_to_2017.csv',dtype=object)
# df_acc['Latitude'] = df_acc['Latitude'].astype(float)
# df_acc['Longitude'] = df_acc['Longitude'].astype(float)
# heat_df = df_acc[['Latitude', 'Longitude']]
# heat_df = heat_df.dropna(axis=0, subset=['Latitude','Longitude'])
# heat_data = [[row['Latitude'],row['Longitude']] for index, row in heat_df.iterrows()]
# #print(heat_data)

# read a csv file of chicago crimes
fd = open('Chicago_Crimes_2012_to_2017.csv', 'r')
csvReader = csv.reader(fd, delimiter=',')
next(csvReader)


# create two lists recording all the coordinates of day and night
heat_data_day = []
heat_data_night = []
for row in csvReader:
    if(row[20] != "" and row[21] != ""):
        lat = float(row[20])
        lon = float(row[21])
        if(selectDay(row[3])):
            heat_data_day.append([lat,lon])
        else:
            heat_data_night.append([lat,lon])

fd.close()
HeatMap(heat_data_day, radius = 12, blur = 18, max_val = 20, min_opacity = 0.5).add_to(my_map_day)
HeatMap(heat_data_night, radius = 12, blur = 18, max_val = 20, min_opacity = 0.5).add_to(my_map_night)

# save the files as html
my_map_day.save('visualization/heatmapDaytime.html')
my_map_night.save('visualization/heatmapNight.html')
