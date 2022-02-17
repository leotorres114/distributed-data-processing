--register the python file.
register 'udf.py' using jython as udfs;

--remove a file in HDFS if the same name file exists.
rmf fare_density_output;

--load data.
raw = LOAD '/geog407/exam2/ny_taxi_1.csv' USING PigStorage(',') AS (col1:chararray,col2:chararray,col3:chararray,col4:int,col5:int,start_date:chararray,end_time:chararray,passenger_count:int,col9:int,col10:double,start_lon:double,start_lat:double,end_lon:double,end_lat:double,col15:chararray,col16:double,col17:double,col18:double,col19:double,col20:double,fare:double);

--get only columns we need
step1 = FOREACH raw GENERATE $0,$7,$10,$11,$20;

--filter out the data based on the bounding box of NYC
step2 = FILTER step1 BY start_lon > -74.25 AND start_lon < -73.70 AND start_lat > 40.49 AND start_lat < 40.92;

--generate new records to send down the pipeline to the next operator.
step3 = FOREACH step2 GENERATE $0,$1,$2,$3,$4;

--generate new records based on lat/lon key
step4 = FOREACH step3 GENERATE $1,$4,(int)((start_lon + 74.25)/0.005) AS longkey,(int)((start_lat - 40.49)/0.005) AS latkey;

--collect all records with the same latitude and longitude key.
step4_group = GROUP step4 BY (latkey,longkey);

--calculate fare density (total fare / # of passengers) for each group.
step5_faredensity = FOREACH step4_group GENERATE group, udfs.getAverage(step4.passenger_count,step4.fare);

--sort data based on the group.
result = ORDER step5_faredensity BY group;

--send the output to a folder in HDFS
store result into 'fare_density_output';
