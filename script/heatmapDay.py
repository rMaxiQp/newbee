import json
import folium
from folium import plugins
from folium.plugins import HeatMap
import os
import time
from selenium import webdriver

# change date from format xx/xx/xxxx to month-year format 
def formatDate(date):
    list = date.split('/')
    newDate = list[0] + '-' +list[1]
    return newDate

fd = open("monthData/2010_month_region_data.txt", 'r')
json_data = json.loads(fd.read())

# create heatmaps and save them as result from formatted date
for month in json_data.keys():
    try:
        coords = json_data.get(month)
        for i in range(len(coords)):
            coords[i] = list(map(lambda x:float(x), coords[i]))
        my_map = folium.Map(location=[41.833778, -87.687908], zoom_start = 11)
        HeatMap(coords, radius = 12, blur = 17, max_val = 1, min_opacity = 0.5, overlay = False).add_to(my_map)
        # save_map(my_map, 'visualization/videoScreenshot/'+formatDate(month)+'.html', formatDate(month)+'.png')
        my_map.save('visualization/videoScreenshot/2010/'+formatDate(month)+'.html')
        print("done")
    except:
        print("error!!!")


fd.close()
