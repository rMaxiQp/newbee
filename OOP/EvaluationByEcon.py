from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
import csv
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt


WEATHER = ["WSF2", "WT13", "WT16", "WT18"]
class EvaluationByEcon(object):
    '''
        Constructor
    '''
    def __init__(self,):
        input_file_econ = "hdfs:///projects/group4/Chicago_gdp.csv"
        input_file_crime = "hdfs:///projects/group4/Chicago_Crimes.csv"

        # Setup Spark
        conf = SparkConf().setAppName("newbee_Chicago_crime_fst")
        self.sc = SparkContext(conf=conf)
        self.sqlContext = SQLContext(self.sc)

        #setup table
        self.setup_table(self.sc, self.sqlContext, input_file_crime, "crime_table")
        self.setup_table(self.sc, self.sqlContext, input_file_econ, "gdp_table")

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
        get the data of weather
    '''
    def evaluate_by_weather(self):
        sql_content = "SELECT gdp FROM " + gdp_table + " ORDER BY year"
        raw_list = self.sqlContext.sql(sql_content).rdd.map(list)
        # mapper = raw_list.filter(lambda x:x[0].isdigit())
        print("econ_raw_list:{}".format(raw_list))
        result = raw_list.take(16)
        print("econ_take16:{}".format(result))
        result = [x[0] for x in result]
        print("retval:{}".format(result))
        return result

    '''
        get total number of criminal cases by year
    '''
    def evaluate_by_crime(self):#, sc, sqlContext):
        '''
            select: helper function of map() to filter data
        '''
        def select(x):
            try:
                return (x, 0)
            except:
                return None

        raw_list = self.sqlContext.sql("SELECT year FROM crime_table WHERE year<=2016").rdd.map(list)
        mapper = raw_list.filter(lambda x:x != None)
        reducer = mapper.map(lambda x: (x[0], 0))
        # print("reducer:{}".format(reducer.take(10)))
        retval = reducer.countByKey()
        return retval
