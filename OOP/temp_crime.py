from operator import add
import numpy as np
matplotlib.use('agg')
import matplotlib.pyplot as plt
import scipy.stats
from matplotlib import dates
from datetime import datetime
import pyspark
from pyspark import SparkContext

def parse_crime(x):
    try:
        return (x[3].split()[0], 1)
    except:
        return None

def combine(x, weather_data):
    try:
        return (float(weather_data[x[0]]), x[1])
    except:
        return None

# def spec(x):
#     try:
#         return (x[0][:5], x[1])
#     except:
#         return None

def convert_time(x):
    xaxis = datetime.strptime(x,'%m/%d/%Y')
    return xaxis

class Crime_Weather(object):
    def __init__(self, weather_path, crime_path):
        self.weather_path = weather_path
        self.crime_path = crime_path

    #3: AWND, 4: PRCP, 5: SNWD, 6: TAVG, 7: TSUN
    def parse_data(self, weather_comp_idx):
        weather_data = spark.read.csv(self.weather_path, header=True).rdd\
                    .map(lambda x: (x[2], x[weather_comp_idx]))\
                    .collectAsMap()
        crime_data = spark.read.csv(self.crime_path, header=True).rdd.map(lambda x : parse_crime(x))\
                    .filter(lambda x: x != None).reduceByKey(add)


#         high_crime = crime_data.filter(lambda x: x[1] > 1900)
#         low_crime = crime_data.filter(lambda x: x[1] < 400)

#         h2 = high_crime.sortBy(lambda a: a[1], ascending=False)
#         l2 = low_crime.sortBy(lambda a: a[1], ascending=False).filter(lambda x:x[0] != 'Case')

        weather_crime = crime_data.map(lambda x : combine(x, weather_data))\
                        .filter(lambda x: x != None)

        values = np.array(weather_crime.collect()).T

        self.x = values[0]
        self.y = values[1]


    def evaluate_corr(self):
        return scipy.stats.pearsonr(self.x,self.y)

    def show_result(self, label):
        plt.figure(1)
        plt.plot(self.x, self.y, 'o')
        plt.xlabel(label)
        plt.ylabel('Number of incidents')
        plt.title('Relationship between number of incidents and '+ label +'\n from 2001 to 2017')
        plt.show()
        file_name = "num_incidents_vs_" + label + ".png"
        plt.savefig(file_name)

#         high = np.array(high.collect()).T
#         high_x = [convert_time(date) for date in high[0]]
#         high_y = high[1]

#         plt.figure(2)
#         plt.plot_date(high_x, high[1], fmt='o', tz=None, xdate=True, ydate=False)
#         plt.xticks(rotation=20)
#         plt.ylabel('Number of incidents')
#         plt.title('Relationship between high crime rate and date\n from 2001 to 2017')
#         plt.show()

#         low = np.array(low.collect()).T
#         low_x = [convert_time(date) for date in low[0]]
#         low_y = low[1]

#         plt.figure(3)
#         plt.plot_date(low_x, low[1], fmt='o', tz=None, xdate=True, ydate=False)
#         plt.xticks(rotation=20)
#         plt.ylabel('Number of incidents')
#         plt.title('Relationship between low crime rate and date\n from 2001 to 2017')
#         plt.show()

sc = pyspark.SparkContext(appName="crime_in_chicago")

# weather_path = '/Users/Jeremy/Desktop/Untitled.csv'
# crime_path = '/Users/Jeremy/Desktop/CS 398/Data/Chicago_Crimes_2001_to_2004.csv'

# weather_path = '1313067.csv'
# crime_path = ['Data/Chicago_Crimes_2001_to_2004.csv',
#               'Data/Chicago_Crimes_2005_to_2007.csv',
#               'Data/Chicago_Crimes_2008_to_2011.csv',
#               'Data/Chicago_Crimes_2012_to_2017.csv']
#
# instance = crime_weather(weather_path, crime_path)
#
# instance.parse_data(4)
#
# instance.show_result('Precipitation')
#
# instance.evaluate_corr()
