import csv
import pygeoj
import geojson
import json
import re
import folium
import pandas as pd

# see what the data looks like
# fd = open('CommAreas.csv', 'r')
# csvReader = csv.reader(fd, delimiter=',')


my_map = folium.Map(location=[41.881832, -87.623177], zoom_start = 10)

# idx 5 is the census track ID
# fd2 =open('byRegion.csv','r')
# csvReader2 = csv.reader(fd2, delimiter=',')

# censusDict = {}
# for row in csvReader2:
#     if (censusDict.get(int(float(row[0]))) == None):
#         censusDict[int(float(row[0]))] = row[1]

# pat = re.compile(r'''(-*\d+\.\d+ -*\d+\.\d+);*''')
# for blk in csvReader:
#     matches = pat.findall(blk[0])
#     if matches:
#         lst = [tuple(map(float, reversed(n.split()))) for n in matches]
#         geoplotlib.convexhull(lst, col = 'red', fill = True, point_size=4)
#
# geoplotlib.savefig('chicago.png')


# my_map.save('chicagoCensus.html')
# read the csv file into a dataframe, k-v pair
df = pd.read_csv('byRegion.csv')

# cast the float into string to make it work
df['area_id'] = df['area_id'].astype(str)

# call choropleth funciton to create visualization
my_map.choropleth(
    geo_data="CommunityAreas.geojson",
    name='choropleth',
    data = df,
    columns=['area_id', 'count'],
    key_on = 'feature.properties.area_num_1',
    fill_color='OrRd',
    fill_opacity=0.7,
    line_opacity=1
)
folium.LayerControl().add_to(my_map)

# geo_json_data = json.load(open('CommunityAreas.geojson'))
# crimeCount = pd.read_csv('byRegion.csv')
# crime_dict = crimeCount.set_index('area_id')['count']
#
# def my_color_function(feature):
#     """Maps low values to green and hugh values to red."""
#     if crime_dict[feature['area_id']] > 100000:
#         return '#ff0000'
#     else:
#         return '#008000'
#
#
# folium.GeoJson(
#     geo_json_data,
#     style_function=lambda feature: {
#         'fillColor': my_color_function(feature),
#         'color': 'black',
#         'weight': 2,
#         'dashArray': '5, 5'
#     }
# ).add_to(my_map)
#
my_map.save('chicagoCensus.html')
