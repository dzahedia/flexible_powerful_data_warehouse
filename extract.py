import sys
import os


from dotenv import load_dotenv
load_dotenv()


from datetime import datetime

from app_conf import *
from app_util import  save_json, getRawData

raw_data = os.getenv('raw_data')


if __name__ == '__main__':
    # argparser is a better option, but this job needs only two arguments, so I go with sys.argv

    year = sys.argv[1]
    mon = sys.argv[2]
    month = f'{year}-{mon}'

    # Here we get the raw data
    # To separate Extract step from Load and Transform, we first finish the download

    dl_files = []
    # I keep newly downloaded files in a list for next step
    # In production and for different type of data, we can call (with Kafka) another process for Load and Transform
    # but here I just keep it in a json file and use it in the next step

    for name in data_types:
        res = getRawData(name, month, base_url, raw_data)
        # for this version I just use print, we can use better logging in production
        print(res['status'], res['msg'])
        if res['status'] == 200:
            dl_files.append(res['out_file'])

    # For our load step, we keep the job status data; this will be used in load step
    # in production, we can use Apache Airflow or micro service architecture to communicate between steps
    job_stat = {'files': dl_files, 'year': year, 'month': mon,
                'updated': datetime.now().isoformat(), 'loaded': False}
    res = save_json(job_stat, latest_dl)
    print(res['status'], res['msg'])




