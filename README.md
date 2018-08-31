# Newbee

Crimes in Chicago evaluation

[Diagrams](https://gitlab.engr.illinois.edu/cs398-sp18-final-project/newbee/tree/master/Data_report_diag)
Above link directs to the folder of visualization graphs that we created earlier.

## Packages
* numpy
* matplotlib
* folium
* pyspark.sql
* selenium
* scipy
* pyspark.rdd

## Framework
* Apache.spark

## By time, econ and weather ([OOP](https://gitlab.engr.illinois.edu/cs398-sp18-final-project/newbee/tree/master/OOP) with spark)

dv_report.py: This class acts as a main function.
We use OOP so that other class can be driven by dv_report.py

Use ``spark-submit dv_report.py`` with following `param` to run the program.

* `param` = time/weather/econ/region/location/adweather

| File Name | Purpose |
| ------------ | ------------ |
| EvaluationByTime.py | get the crime data grouped by time. |
| EvaluationByEcon.py | get the crime data grouped by year and econ data by year |
| EvaluationByWeather.py | join weather and crime by date so and combine crime data with weather data by date |
| temp_crime.py | make an advanced analysis on crime and weather |

## By location ([script](https://gitlab.engr.illinois.edu/cs398-sp18-final-project/newbee/tree/master/script))

In this analysis, since we are using external library that cannot be installed in cluster,
we have to implement it locally.

| File Name | Purpose |
| ----------| ------- |
| visualRegionChoro.py | create a choropleth based on the geojson and csv files |
| heatmapDayNight.py | create 2 heat maps based on hours |
| heatmapSeason.py | create 4 heat maps based on season |
| heatmaplocal.py | create a heap map based on location |
| heatmapDay.py | create heamaps by moth for a given year |
| createSS.py | get sreenshots of an interactive html files |

Use ``python`` with following `FILENAME` to run the program.
