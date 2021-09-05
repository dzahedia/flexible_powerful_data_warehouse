# a simple app config, in production, a combination of .env, yaml or json would do the job

base_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/"

latest_dl = 'last_extracted_job.json'
latest_load ='last_loaded_job.json'

data_types = ['yellow_tripdata_', 'green_tripdata_'] # ignore the other files: 'fhv_tripdata_', 'fhvhv_tripdata_'

columns_to_drop = ['tpep_pickup_datetime', 'lpep_pickup_datetime', 'tpep_dropoff_datetime', 'lpep_dropoff_datetime']

