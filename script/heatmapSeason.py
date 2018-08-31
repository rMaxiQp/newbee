import csv
import pandas as pd
import folium
from folium import plugins
from folium.plugins import HeatMap

# 3-5 spring, 6-8 summer, 9-11 fall, 12-2 winter.
def selectSeason(x):
    raw = x.split()
    if(len(raw) != 3):
        return -1

    month = int(raw[0].split("/")[0])

    if 3 <= month and month <=5:
        return "spring"
    elif 6 <= month and month <= 8:
        return "summer"
    elif 9 <= month and month <= 11:
        return "autumn"
    else:
        return "winter"

fd = open('Chicago_Crimes_2012_to_2017.csv', 'r')
my_map_spring = folium.Map(location=[41.881832, -87.623177], zoom_start = 13)
my_map_summer = folium.Map(location=[41.881832, -87.623177], zoom_start = 13)
my_map_autumn = folium.Map(location=[41.881832, -87.623177], zoom_start = 13)
my_map_winter = folium.Map(location=[41.881832, -87.623177], zoom_start = 13)


fd = open('Chicago_Crimes_2012_to_2017.csv', 'r')
csvReader = csv.reader(fd, delimiter=',')
next(csvReader)

heat_data_spring = []
heat_data_summer = []
heat_data_autumn = []
heat_data_winter = []

# create heatmaps based on the month and aggregate them into season
for row in csvReader:
    if(row[20] != "" and row[21] != ""):
        lat = float(row[20])
        lon = float(row[21])
        season = selectSeason(row[3])
        if season == "spring":
            heat_data_spring.append([lat,lon])
        elif season == "summer":
            heat_data_summer.append([lat,lon])
        elif season == "autumn":
            heat_data_autumn.append([lat,lon])
        elif season == "winter":
            heat_data_winter.append([lat,lon])
        else:
            pass

fd.close()

HeatMap(heat_data_spring, radius = 12, blur = 18, max_val = 20, min_opacity = 0.5).add_to(my_map_spring)
HeatMap(heat_data_summer, radius = 12, blur = 18, max_val = 20, min_opacity = 0.5).add_to(my_map_summer)
HeatMap(heat_data_autumn, radius = 12, blur = 18, max_val = 20, min_opacity = 0.5).add_to(my_map_autumn)
HeatMap(heat_data_winter, radius = 12, blur = 18, max_val = 20, min_opacity = 0.5).add_to(my_map_winter)

my_map_spring.save('visualization/heatmapSpring.html')
my_map_summer.save('visualization/heatmapSummer.html')
my_map_autumn.save('visualization/heatmapAutumn.html')
my_map_winter.save('visualization/heatmapWinter.html')
