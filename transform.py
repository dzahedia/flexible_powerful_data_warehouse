import os
import json

from app_conf import latest_load
from app_util import transformMonth, save_json
from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession


dwh_data = os.getenv('dwh_data')
gold_data = os.getenv('gold_data')

if __name__ == '__main__':

    with open(latest_load) as f:
        last_job = json.loads(f.read())

    if last_job['transformed']:
        print('Nothing to transform!')
        exit(1)
    print('starting new transfer ...')

    spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "8g").appName('tlc_load').getOrCreate()

    month = f"{last_job['year']}-{last_job['month']}-01"

    res = transformMonth(spark, month, dwh_data, gold_data)
    print(res['status'], res['msg'])
    if res['status'] == 500:
        raise Exception(res['msg'])

    last_job['transformed'] = True
    res = save_json(last_job, latest_load)
    print(res['status'], res['msg'])