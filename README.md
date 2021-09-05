AN ELT pipeline for TLC data 

- Whenever necessary, I added comment for a production code; this is just a prototype version 0.X
- Since, it is not clear how often they upload new data, I implemented a module to dynamically get and process data for a given month.
- To download and process data for any month just pass year and month as argument to extract.py. For example, data in raw_data folder is downloaded by 'extract.py 2020 12' command (so it is tested).

- add JAVA_HOME to .env file