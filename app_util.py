import json
import requests

from app_conf import columns_to_drop

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType



def getRawData(name, month, base_url, out_dir):

    """
    :param name: the prefix in TLC
    :param month: a string in yyyy-mm format
    :param base_url:
    :param out_dir:
    :return:
    """


    url = base_url + name + month + ".csv"
    out_file= out_dir + '/' + name + month + '.csv' # in production this can be done better with os.path

    res = requests.get(url, allow_redirects=True)
    if res.status_code != 200:
        return {'status': res.status_code, 'msg': 'Failed to download {}'.format(out_file), 'out_file':'NA'}
    with open(out_file, 'wb') as f:
        f.write(res.content)
    return {'status': 200, 'msg': 'Saved {}'.format(out_file), 'out_file':out_file}


def loadFromFile(spark, file, color, month_dt, dwh_data, columns_to_drop=columns_to_drop):
    """
    :param spark: spark session
    :param file: raw data origin
    :param color: taxi color
    :param month_dt: month_dt a datetime object the first day of the month to load
    :param dwh_data:
    :param columns_to_drop: a list of redundant columns to drop; for example because of renaming four columns are extra in this case
    :return:
    """

    try:
        df = spark.read.option("header", "true").csv(file, inferSchema=True)
        df = df.withColumn('color', lit(color))
        df = df.withColumn('month', lit(month_dt))
        if color == 'yellow':
            df = df.withColumn('pickup_datetime', to_timestamp('tpep_pickup_datetime'))
            df = df.withColumn('dropoff_datetime', to_timestamp('tpep_dropoff_datetime'))
        else:
            df = df.withColumn('pickup_datetime', to_timestamp('lpep_pickup_datetime'))
            df = df.withColumn('dropoff_datetime', to_timestamp('lpep_dropoff_datetime'))

        df = df.drop(*columns_to_drop)
        df.write.partitionBy("month", "color").mode("append").parquet(dwh_data)
    except Exception as e:
        return {'status': 500, 'msg': e}
    return {'status': 200, 'msg': f'{color + month_dt.isoformat()[:7]} load completeded.'}

hour = udf(lambda x: x.hour, IntegerType())

def transformMonth(spark, month, dwh_data, gold_data):
    """
    :param spark: spark session object
    :param month: month in string format yyyy-mm-01
    :param dwh_data:
    :param gold_data:
    :return:
    """

    try:
        cols = ['VendorID', 'color', 'date', 'passenger_count', 'trip_distance', 'hour', 'day', 'month']

        df = spark.read.option('mergeSchema', 'true').parquet(dwh_data)
        print('all loaded data length: ', df.count())
        df = df.filter(f"month='{month}'")
        print('new loaded data length: ', df.count())

        df_c = df.withColumn('date', to_date('pickup_datetime'))
        df_c = df_c.withColumn('hour', hour('pickup_datetime'))

        df_c = df_c.withColumn('day', date_format('date', 'EEEE'))
        df_gold = df_c.select(cols)

        print('Transforming and saving results...')
        df_gold.write.partitionBy("month", "color").mode("append").parquet(gold_data)

    except Exception as e:
        return {'status': 500, 'msg': e}
    return {'status': 200, 'msg': f'{month} is transformed and saved.'}

def transformAVRO(spark, dwh_data, filter_statement, avro_data, schema):
    """
    :param spark:
    :param dwh_data:
    :param filter_statement: a string filter including the field and condition like: 'hour=9'
    :param avro_data:
    :param schema: avro schema with fields present in the dwh_data table
    :return:
    """
    pass

def save_json(dict, filename):
    with open(filename, 'w') as f:
        f.write(json.dumps(dict))
    return {'status': 200, 'msg':'Saved {}'.format(filename)}