**A data warehouse implemented with Spark, Parquet and HDFS** 
    - with an example from New York taxis data
- flexibly handles the evolving data schema
- quickly handles complex queries (queriesLog.txt)
- scalably extends


**To use:**
- install requirements
- add .env file to include raw_data, dwh_data and gold_data locations (can be HDFS or local file system or other system that can be connected with Spark)

**For example:**

New York Taxis data are available monthly with varying schema. This is not a production pipeline! Just a proof of concept; so no validation steps or good logs (sampleLog.txt file is provided)! 

The implemented pipeline here can be used to **Extract**, **Load** and **Transfor**m data for any period of time including a single month.

To use the pipeline, the following command would get the data from 2018-02 to 2020-03.

`python batch_processing.py --start_year 2018 --start_month 2 --end_year 2020 --end_month 3 --job_type t
`



job_type can be **_e_** for only extract, **_l_** for **_e_** and load and **_t_** for **_l_** and transform

**Alternatively**

you can call (in sequence) any of the steps modules (extract.py, load.py and transform.py) manually for a single month. 
  
**Scalabe:**

- Because all the steps are seperate, one can easily add new transforms, for example. 
- Or, one can add new dataset like weather data and join it with a new transform step. 

**To extend:**

The core of this app is the app_util: it contains three main functions for each step of ELT. One can employ those functions in other pipelines implemented in: 

    - Apache Airflow
    - Micro services pipeline
    - and etc.
  