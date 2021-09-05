**A data warehouse implemented with New York taxis data**

- flexibly handles the evolving data schema
- quickly handles complex queries 
- scalably extends


**To use:**
- install requirements
- add .env file to include raw_data, dwh_data and gold_data locations

Data are available monthly and you can use batch_processing.py to Extract, Load and Transform data for any period of time including a single month

**for example:**

This command: 

`python batch_processing.py --start_year 2018 --start_month 2 --end_year 2020 --end_month 3 --job_type t
`

would get the data from 2018-02 to 2020-30

job_type can be **_e_** for only extract, **_l_** for **_e_** and load and **_t_** for **_l_** and transform
  
**Scalabe:**

- Because all the steps are seperate, one can easily add new transforms, for example. 
- Or, one can add new dataset like weather data and join it with a new transform step. 

    

  