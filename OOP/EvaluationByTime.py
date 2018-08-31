from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
import csv
import matplotlib
import numpy as np
matplotlib.use('agg')
import matplotlib.pyplot as plt


SERIOUS = ['OFFENSE INVOLVING CHILDREN',
'PUBLIC PEACE VIOLATION',
'ARSON',
'CRIMINAL TRESPASS',
'ASSAULT',
'ROBBERY',
'HOMICIDE',
'CRIM SEXUAL ASSAULT',
'HUMAN TRAFFICKING',
'INTIMIDATION',
'CRIMINAL DAMAGE',
'KIDNAPPING',
'BURGLARY',
'WEAPONS VIOLATION']

class EvaluationByTime(object):
    def __init__(self):
        input_file = "hdfs:///projects/group4/Chicago_Crimes.csv"

        # Setup Spark
        conf = SparkConf().setAppName("newbee_Chicago_crime_fst")
        self.sc = SparkContext(conf=conf)
        self.sqlContext = SQLContext(self.sc)

        #setup table
        self.setup_table(self.sc, self.sqlContext, input_file)

    '''
        helperfunction: help set up two tables
    '''
    def setup_table(self, sc, sqlContext, reviews_filename):
    '''
        Args:
            sc: The Spark Context
            sqlContext: The Spark SQL context
            reviews_filename: input dataset file
            table_name: table name
    '''
        df = sqlContext.read.csv(reviews_filename, header=True, inferSchema=True)
        sqlContext.registerDataFrameAsTable(df, 'table')


    def evaluate_serious_by_hour(self): # sc, sqlContext):
        '''
        Args:
            sc: The Spark Context
            sqlContext: The Spark SQL context
        Returns:
            A list: the total number of crimes by hour
        '''
        def select(x):
            raw = x.split()
            if(len(raw) != 3):
                return None

            if(raw[2] == "PM"):
                value = (int(raw[1][:2]) + 12) % 24
                return (str(value) , 0)
            else:
                return (raw[1][:2], 0)

        raw_list = self.sqlContext.sql("SELECT date, `Primary Type` FROM table").rdd.map(list)
        filt = raw_list.filter(lambda x: x[1] in SERIOUS)
        mapper1 = filt.map(lambda x: x[0])
        mapper2 = mapper1.map(select)
        filt1 = mapper2.filter(lambda x : x != None)
        retval = filt1.countByKey()
        return retval

    def evaluate_by_hour(self):
        '''
        Args:
            sc: The Spark Context
            sqlContext: The Spark SQL context
        Returns:
            A list: the total number of crimes by hour
        '''
        def select(x):
            raw = x.split()
            if(len(raw) != 3):
                return None

            if(raw[2] == "PM"):
                value = (int(raw[1][:2]) + 12) % 24
                return (str(value) , 0)
            else:
                return (raw[1][:2], 0)

        raw_list = self.sqlContext.sql("SELECT date FROM table").rdd.map(list)
        mapper1 = raw_list.map(lambda x: x[0])
        mapper2 = mapper1.map(select)
        filt = mapper2.filter(lambda x : x != None)
        retval = filt.countByKey()
        return retval



    def evaluate_by_year(self):#, sc, sqlContext):
        '''
        Args:
            sc: The Spark Context
            sqlContext: The Spark SQL context
        Returns:
            A list: the total number of crimes by year
        '''
        def select(x):
            try:
                return (x, 0)
            except:
                return None

        raw_list = self.sqlContext.sql("SELECT year, COUNT(*) FROM table WHERE year>=2001 AND year<= 2016 GROUP BY year ORDER BY year").rdd.map(list)
        filt = raw_list.filter(lambda x:x[0]!=None and x[1]!=None)
        mapper = filt.map(lambda x: x[1])
        retval = mapper.take(16)
        # print("years_filt_output:{}".format(filt.take(22)))
        # print("year_retval_rdd:{}".format(mapper.take(22)))
        # print("year_retval:{}".format(retval))
        return retval

    def evaluate_serious_by_year(self):#, sc, sqlContext):
        '''
        Args:
            sc: The Spark Context
            sqlContext: The Spark SQL context
        Returns:
            A list: the total number of crimes by year
        '''
        def select(x):
            try:
                return (x, 0)
            except:
                return None

        raw_list = self.sqlContext.sql("SELECT year, `Primary Type`FROM table WHERE year>=2001 AND year<= 2016 ORDER BY year").rdd.map(list)
        filt = raw_list.filter(lambda x:x[0]!=None and x[1]!=None)
        filt1 = filt.filter(lambda x: x[1] in SERIOUS)
        mapper = filt1.map(lambda x: x[0])
        mapper1 = mapper.map(select)
        filt2 = mapper1.filter(lambda x : x != None)
        retval = filt2.countByKey()
        # print("serious_year:{}".format(retval))
        return retval


    def evaluate_by_season(self): #, sc, sqlContext):
        '''
        Args:
            sc: The Spark Context
            sqlContext: The Spark SQL context
        Returns:
            A list: the total number of crimes by seasons
        '''
        def select(x):
            raw = x.split()
            if(len(raw)!=3):
                return None

            year = raw[0][:2]
            if(year >= "03" and year <= "05"):
                return (0, 0)                   ## Spring
            elif(year >= "06" and year <= "08"):
                return (1, 0)                   ## Summer
            elif(year >= "09" and year <= "11"):
                return (2, 0)                   ## Fall
            else:
                return (3, 0)                   ## Winter

        raw_list = self.sqlContext.sql("SELECT date "
                                       "FROM table").rdd.map(list)
        mapper1 = raw_list.map(lambda x: x[0])
        mapper2 = mapper1.map(select)
        filt = mapper2.filter(lambda x : x != None)
        retval = filt.countByKey()
        return retval

    def evaluate_serious_by_season(self): #, sc, sqlContext):
        '''
        Args:
            sc: The Spark Context
            sqlContext: The Spark SQL context
        Returns:
            A list: the total number of crimes by seasons
        '''
        def select(x):
            raw = x.split()
            if(len(raw)!=3):
                return None

            year = raw[0][:2]
            if(year >= "03" and year <= "05"):
                return (0, 0)                   ## Spring
            elif(year >= "06" and year <= "08"):
                return (1, 0)                   ## Summer
            elif(year >= "09" and year <= "11"):
                return (2, 0)                   ## Fall
            else:
                return (3, 0)                   ## Winter

        raw_list = self.sqlContext.sql("SELECT date, `Primary Type` "
                                       "FROM table").rdd.map(list)
        filt = raw_list.filter(lambda x: x[1] in SERIOUS)
        mapper1 = filt.map(lambda x: x[0])
        mapper2 = mapper1.map(select)
        filt1 = mapper2.filter(lambda x : x != None)
        retval = filt1.countByKey()
        return retval


    def get_location_by_date(self, year):
        sql_content = "SELECT date, SUBSTR(date,1,2) AS month, SUBSTR(date,7,4), Latitude, Longitude " + \
                      "FROM table " + \
                      "WHERE year=" + str(year) + " " + \
                      "ORDER BY month"
        print("sql:{}".format(sql_content))
        raw_list = self.sqlContext.sql(sql_content).rdd.map(list)
        print("take5:{}".format(raw_list.take(5)))
        filter1 = raw_list.filter(lambda x: x[0]!=None and x[3]!=None and x[4]!=None and len(x[2])==4)
        mapper1 = filter1.map(lambda x: (str(x[1])+"/"+str(x[2]), (x[3], x[4])))
        print("mapper:{}".format(mapper1.take(5)))
        return mapper1.groupByKey().mapValues(list).collect()


    # if __name__ == '__main__':
    #     # Get input/output files from user
    #     parser = argparse.ArgumentParser()
    #     parser.add_argument('input', help='File to load data from')
    #     args = parser.parse_args()
    #
    #     # Setup Spark
    #     conf = SparkConf().setAppName("newbee_Chicago_crime_fst")
    #     sc = SparkContext(conf=conf)
    #     sqlContext = SQLContext(sc)
    #
    #     #setup table
    #     setup_table(sc, sqlContext, args.input)
    #
    #     result = evaluate_by_hour(sc, sqlContext)
    #     print(result)
    #     #result.saveAsTextFile("output.txt")
