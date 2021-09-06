import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col

gold_data = os.getenv('gold_data')

spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "8g").appName('tlc_query').getOrCreate()

df = spark.read.option("mergeSchema", "true").parquet(gold_data)

print('Number of rows: ', df.count())

# query to get the average trip distance for color and hour
start_time = datetime.now()
df.select(['color', 'hour', 'trip_distance']).groupby(['color', 'hour']).avg('trip_distance').orderBy(['hour', 'color']).show(50)
print('time to query: ', datetime.now()-start_time)


#query to get the avg passenger count for year and day
start_time = datetime.now()
df.select(year("date").alias('year'), 'day', 'passenger_count').groupby(['year', 'day']).avg('passenger_count').orderBy(['year', 'day']).show()
print('time to query: ', datetime.now()-start_time)

# query to get top three busy hours
start_time = datetime.now()
df.select('VendorID', 'hour').groupby('hour').count().orderBy(col('count').desc()).show(3)
print('time to query: ', datetime.now()-start_time)



