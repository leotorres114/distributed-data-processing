import sys
import time
from pyspark import SparkContext
import math
import numpy as np

def makeIndex(record):
    '''filter and indexing target point records'''
    
    point_data = record.strip().split(",")

    # Lat Filter
    if point_data[3] == "":
        return []

    lat = float(point_data[3])

    if lat < start_lat or lat > end_lat:
        return []

    # Lon Filter
    if point_data[4] == "":
        return []

    lon = float(point_data[4])

    if lon < start_lon or lon > end_lon:
        return []

    row_index = int((lat - start_lat) / grid_size)
    col_index = int((lon - start_lon) / grid_size)

    return [((row_index, col_index),1)]

if __name__ == "__main__":
    sc = SparkContext(appName="Chicago311Raster")
    dataset = sc.textFile('/geog407/exam2/311_data.csv').filter(lambda x: True).cache()
    paras = sys.argv[-5:]

    start_lat = float(paras[0])
    end_lat = float(paras[1])
    start_lon = float(paras[2])
    end_lon = float(paras[3])
    grid_size = float(paras[4])

    result = dataset.flatMap(makeIndex).reduceByKey(lambda a,b: (a+b))
 
    result.saveAsTextFile('Exam2/Spark/chicago311_raster_%s.txt' %str(time.time()))
    print('[Done]!')
