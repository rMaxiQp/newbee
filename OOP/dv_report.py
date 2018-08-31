from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
import csv
import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import json
import sys
from EvaluationByTime import EvaluationByTime
from EvaluationByEcon import EvaluationByEcon
from EvaluationByWeather import EvaluationByWeather
from temp_crime import Crime_Weather


'''
    create a plot graph with given inputs
'''
def create_plot_graph(time ,dd, y_name, x_name, x_ticks=None, title=None):
    '''
        Args:
            time: x_axis
            dd: y_values
            y_name: label of y_axis
            x_name: label of x_axis
            x_ticks: the label of each x values
            x_ticks: the label of each y values
        Output: the plot diagram
    '''
    total_np = []
    try:
        total_np = np.array([x for _,x in sorted(dd.items(), key=lambda x:x[0])])
    except:
        total_np = np.array(dd)

    plt.figure(figsize=(20,10))
    fig_title = y_name + " V.S. " + x_name
    if title != None:
        fig_title += "\n" + "Serious Crimes"
    plt.title(fig_title)
    plt.xlabel(x_name)
    plt.ylabel(y_name)
    plt.plot(time, total_np, 'b-')
    plt.plot(time, total_np, 'ro')
    plt.xticks(time)
    fig_name = y_name + "_vs_" + x_name + ".png"
    if title != None:
        fig_name = "serious_crime_" + y_name + "_vs_" + x_name + ".png"
    plt.savefig(fig_name)


'''
    create a bar graph with given inputs
'''
def create_bar_graph(x_ticks, x_label, x_values, y_ticks, y_label, y_values):
    '''
        Args:
            x_ticks: the label of each x values
            x_label: the label of x-axis
            x_values: x values
            y_ticks: the label of each x values
            y_label: the label of y-axis
            y_values: y values
        Output: the bar diagram
    '''
    try:
        plt.bar(x_values, y_values, align='center')
        plt.yticks(y_values, y_ticks)
        plt.xticks(x_values, x_ticks)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.title(y_label + " V.S. " + x_label)
        plt.savefig(y_label + "_vs_" + x_label + ".png")
    except:
        print("ERROR")

'''
    helperfunction: convert dictionary to list with sorted key
'''
def get_list_from_dict(d):
    l = [(k,v) for k,v in d.items()]
    a = sorted(l, key=lambda x: x[0])
    b = [x[1] for x in a]
    return b

'''
    Use function in EvaluationByTime to get the crime data by time
    and use matplotlib to draw the diagrams
'''
def diag_by_time():
    time_year = np.arange(2001,2017,1)
    time_season = np.arange(0,4,1)
    time_hour = np.arange(0,24,1)
    model = EvaluationByTime()
    seasons = ["Spring", "Summer", "FALL", "Winter"]
    total_hour = model.evaluate_by_hour()
    total_hour = get_list_from_dict(total_hour)
    serious_total_hour = model.evaluate_serious_by_hour()
    serious_total_hour = get_list_from_dict(serious_total_hour)
    total_season = model.evaluate_by_season()
    total_season = get_list_from_dict(total_season)
    serious_total_season = model.evaluate_serious_by_season()
    serious_total_season = get_list_from_dict(serious_total_season)
    total_year = model.evaluate_by_year()
    serious_total_year = model.evaluate_serious_by_year()
    serious_total_year = get_list_from_dict(serious_total_year)


    create_plot_graph(time=time_hour, dd=total_hour, y_name="Total", x_name="Hour")
    create_plot_graph(time=time_hour, dd=serious_total_hour, y_name="Total", x_name="Hour", title=1)
    create_plot_graph(time=time_season, dd=total_season, y_name="Total", x_name="Season", x_ticks=seasons)
    create_plot_graph(time=time_season, dd=serious_total_season, y_name="Total", x_name="Season", x_ticks=seasons, title=1)
    create_plot_graph(time=time_year, dd=total_year, y_name="Total", x_name="Year")
    create_plot_graph(time=time_year, dd=serious_total_year, y_name="Total", x_name="Year", title=1)

'''
    Use function in EvaluationByTime to get the data of econ and time(year)
    and use matplotlib to draw two plots of data in one diagram
'''
def diag_by_weather():

    x_values = np.arange(0,5,1)
    x_ticks = ["Rain", "Snow", "Wind", "Mist", "Other"]
    x_label = "Weather"

    model = EvaluationByWeather()
    y_values = model.evaluate_by_weather()
    y_label = "Total_Number"
    create_bar_graph(x_ticks=x_ticks, x_label=x_label, x_values=x_values, y_ticks=None, y_label=y_label, y_values=y_values)


'''
    Use function in EvaluationByTime to get the data of econ and time(year)
    and use matplotlib to draw two plots of data in one diagram
'''
def diag_by_econ():

    #receive data
    model = EvaluationByEcon()
    v = model.evaluate_by_crime()

    crime_values = np.array([x for a,x in sorted(v.items(), key = lambda x: x[0]) if (float(a) > 2000.0)])

    econ = np.array([414435, 423186, 436293, 460632, 486302, 513061, 534765, 528040, 516764, 529004, 547626, 578016, 585948, 608723, 635054, 651222])/100000
    rate = []
    for i in range(len(econ) - 1):
        rate.append((econ[i+1]-econ[i])/econ[i])
    rate.append(rate[-1])
    rate = np.array(rate)
    time = np.arange(2001, 2017, 1)

    #plot
    plt.figure(figsize=(1200,500))
    fig, ax = plt.subplots()
    axes = [ax, ax.twinx(), ax.twinx()]
    fig.subplots_adjust(left = 0.3)
    axes[1].spines['right'].set_position(('axes', -0.5))

    axes[-1].set_frame_on(True)
    axes[-1].patch.set_visible(False)

    fig = plt.figure(figsize=(20,10))
    ax1 = fig.add_subplot(111)
    ax1.plot(time, crime_values, 'b-', label='Crime_Count')
    ax1.set_ylabel('Crime Count', color='b')
    ax1.plot(time, crime_values, 'bo')
    ax1.tick_params(axis='y', colors='blue')

    ax2 = ax1.twinx()
    ax2.plot(time, rate, 'r-', label='GDP_Rate')
    ax2.set_ylabel('GDP_Rate', color='r')
    ax2.spines['right'].set_color('red')
    ax2.plot(time, rate, 'ro')
    ax2.tick_params(axis='y', colors='red')

    ax1.set_xlabel('Year')


    plt.title('Crimes V.S. Economy')

    plt.savefig("Crimes_vs_Econ.png")

'''
    Use function in EvaluationByTime to get the locations of every crimes grouped by month
    and materialize the data to a txt file. Since we use a graph library that cannot implemented
    in cluster, we have to get the output.
'''
def get_region_by_time():
    model = EvaluationByTime()
    for year in range(2001, 2018, 1):
        pairs = model.get_location_by_date(year)
        dic = dict((x[0],x[1]) for x in pairs)
        with open(str(year)+'_month_region_data.txt', 'w') as outfile:
            json.dump(dic, outfile)

def get_adweather(weather):
    weather_path = '1313067.csv'
    crime_path = ['Data/Chicago_Crimes_2001_to_2004.csv',
                  'Data/Chicago_Crimes_2005_to_2007.csv',
                  'Data/Chicago_Crimes_2008_to_2011.csv',
                  'Data/Chicago_Crimes_2012_to_2017.csv']

    instance = Crime_Weather(weather_path, crime_path)
    instance.parse_data(weather)
    if weather==3:
        label = "Wind_Speed"
    elif weather==4:
        label = "Precipitation"
    elif weather==5:
        label = "Snow_Depth"
    elif weather==6:
        label = "Temperature"
    elif weather==7:
        label = "Sunshine"
    instance.show_result(label)
    print("evaluate_corr:{}".format(instance.evaluate_corr()))

def create_report(para):
    para = para[0]
    if para == "time":
        diag_by_time()
    elif para == "weather":
        diag_by_weather()
    elif para == "econ":
        diag_by_econ()
    elif para == "region":
        get_region_by_time()
    elif para == "location":
        get_locations()
    elif para == "adweather":
        #3: AWND, wind 4: PRCP, Precipitation, 5: SNWD, snowdepth 6: TAVG, temperature 7: TSUN sunshine
        weather = para[1]
        get_adweather(weather)


if __name__ == '__main__':
    create_report(sys.argv[1:])
