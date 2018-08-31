from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
import csv
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt


WEATHER = ["WT16", "WT18","WSF2","WT13","OTHER"]


class EvaluationByWeather(object):
    def __init__(self):

        input_file_weather = "hdfs:///projects/group4/weather.csv"
        input_file_crime = "hdfs:///projects/group4/Chicago_Crimes.csv"

        # Setup Spark
        conf = SparkConf().setAppName("newbee_Chicago_crime_fst")
        self.sc = SparkContext(conf=conf)
        self.sqlContext = SQLContext(self.sc)

        #setup table
        self.setup_table(self.sc, self.sqlContext, input_file_crime, "crime_table")
        self.setup_table(self.sc, self.sqlContext, input_file_weather, "weather_table")

    '''
        helperfunction: help set up two tables
    '''
    def setup_table(self, sc, sqlContext, reviews_filename, table_name):
        '''
            Args:
                sc: The Spark Context
                sqlContext: The Spark SQL context
                reviews_filename: input dataset file
                table_name: table name
        '''
        df = sqlContext.read.csv(reviews_filename, header=True, inferSchema=True)
        sqlContext.registerDataFrameAsTable(df, table_name)

    '''
        get the number of crimes for each most common weather
    '''
    def evaluate_by_weather(self): # sc, sqlContext):
        '''
            Return:
                A list that contain the number of crimes for each most common weather
        '''
        retval = []
        for w in WEATHER:
            if w != "OTHER" and w != "WSF2":
                sql = "SELECT COUNT(*) " + \
                      "FROM (SELECT DISTINCT(date) FROM weather_table WHERE " + w + "=1) AS wt " + \
                      "LEFT JOIN crime_table AS ct " + \
                      "ON wt.date = ct.date"
                # print("sql:{}".format(sql))
                wea_day_list = self.sqlContext.sql(sql).rdd.map(list)
                weather_dates = wea_day_list.take(1)
                retval.append(weather_dates[0][0] * 10 + 7)
                # print("weather_dates:{}".format(weather_dates))

            elif w == "WSF2":
                sql = "SELECT COUNT(*) " + \
                      "FROM (SELECT DISTINCT(date) FROM weather_table WHERE " + w + ">25) AS wt " + \
                      "LEFT JOIN crime_table AS ct " + \
                      "ON wt.date = ct.date"
                # print("sql:{}".format(sql))
                wea_day_list = self.sqlContext.sql(sql).rdd.map(list)
                weather_dates = wea_day_list.take(1)
                retval.append(weather_dates[0][0] * 10 + 1)
                # print("weather_dates:{}".format(weather_dates))
            else:
                sql = "SELECT COUNT(*) " + \
                      "FROM (SELECT DISTINCT(date) FROM weather_table WHERE " + WEATHER[2] + "<25 AND " + WEATHER[0] + "<>1 AND " + WEATHER[1] + "<>1 AND " + WEATHER[3] + "<>1) AS wt " + \
                      "LEFT JOIN crime_table AS ct " + \
                      "ON wt.date = ct.date"
                # print("sql:{}".format(sql))
                wea_day_list = self.sqlContext.sql(sql).rdd.map(list)
                weather_dates = wea_day_list.take(1)
                retval.append(weather_dates[0][0] * 10 + 2)
                # print("weather_dates:{}".format(weather_dates))
        # print("retval:{}".format(retval))
        return retval



    def evaluate_by_crime(self):#, sc, sqlContext):
        '''
            Returns:
                A list: the total number of crimes by year
        '''
        def select(x):
            try:
                return (x, 0)
            except:
                return None

        raw_list = self.sqlContext.sql("SELECT year FROM table").rdd.map(list)
        mapper = raw_list.filter(lambda x:x != None)
        reducer = mapper.map(lambda x: (x[0], 0))
        reducer1 = reducer.filter(lambda x: x[0] >= "2001" and x[0] <= "2017")
        # print("reducer:{}".format(reducer.take(10)))
        retval = reducer1.countByKey()
        return retval
