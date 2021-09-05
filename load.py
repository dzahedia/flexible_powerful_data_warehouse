import os
import json
from datetime import datetime

from app_conf import latest_dl, latest_load
from app_util import save_json, loadFromFile

from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

dwh_data = os.getenv('dwh_data')

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "8g").appName('tlc_load').getOrCreate()

    with open(latest_dl) as f:
        last_job = json.loads(f.read())

    if last_job['loaded']:
        print('The files are already in DWH')
        exit(1)

    for f in last_job['files']:
        color = f.strip().split('/')[-1].split('_')[0]
        raw_month = f.strip().split('/')[-1].split('_')[-1].replace('.csv', '')
        year = int(raw_month.split('-')[0])
        month = int(raw_month.split('-')[1])
        month_dt = datetime(year, month, 1)
        res = loadFromFile(spark, f, color, month_dt, dwh_data)
        print(res['status'], res['msg'])
        if res['status'] == 500:
            raise Exception(res['msg'])

    last_job['loaded'] = True
    res = save_json(last_job, latest_dl)
    print(res['status'], res['msg'])

    #keep the latest load for transform

    job_stat = { 'year': last_job['year'], 'month': last_job['month'],
                    'updated': datetime.now().isoformat(), 'transformed': False}

    res = save_json(job_stat, latest_load)
    print(res['status'], res['msg'])

