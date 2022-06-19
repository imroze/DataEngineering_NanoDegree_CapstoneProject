# Udacity Data Engineering Nanodegree - Capstone Project

This repo is for my submission of capstone project of the Data Engineering Nanodegree by Udacity. It is allowed and required to be posted on Github.

### Project Overview

The project scope is mostly open-ended, with certain requirements to follow. It uses data from sources provided by Udacity as well as additional data.

Following scope was defined for the project:

The Govt of State of Georgia is interested in performing analytics on the people immigrating to Georgia, USA for employment, on employee visa i.e. E1, E2 and E3. 

They want to aggregate analytics about employee immigrations on monthly and yearly basis for Georgia cities and airports, based on visa-type and demographics information like age group and gender. They are also interested in airline and flight information.  

Following three datasets are used and combined in the project:

1. Airport Code Table, a CSV file containing information about airports, their codes and corresponding cities. It comes from datahub.io.

2. I94 Immigration Data, as SAS (sas7bdat) files containing information about immigrations to USA for months. It comes from US National Tourism and Trade Office. 

3. Airlines Data, as a JSON file. It comes from https://github.com/npow/airline-codes.

The end solution will be a Data Wareshouse with tables that will be used for OLAP according to requirement and updated monthly using ETL on the raw data.

This project utilizes concepts and lessons from the Nanodegree, including data modeling, data warehousing, ETL and data pipelines.   

The tools and libraries used in this project include Python, PostgreSQL, Psycopg, Pandas, PySpark and Apache AirFlow. 

It is assumed that new SAS data for the month will be available for ingestion every month as SAS files and it has to be converted to CSV files for analysis and easy use with PySpark. The data engineering pipeline will do the ETL and load the data into database every month.

Dimensional Modeling is being done and a Star Schema was created. The tables are stored in a PostgreSQL relational database. 

Check "Captstone_Project_Imroze.ipynb" for detailed project write-up. 

### Data Pipeline Steps

Following steps are carried out in the pipeline:

1. Data for current month is converted from SAS to CSV format and saved. Current month is determined using time tables, except for first time run, and time tables are updated.

2. Loading the dimensions data from csvs and json, and cleaning it, if it's first time run. For next time runs, some data is loaded from the databases.

3. Extracting immigration data for current date in PySpark DataFrame according to schema and cleaning it.

4. Filtering immigration data to get the smaller required data of Georgia employee immigrations.

5. Extracting Day of Month information from numeric SAS Date Format.

6. Age is derived from Birth Year and also classified into Age Group.

7. Immigration DataFrame is Joined with airports data.

8. Immigration DataFrame is Joined with airlines data.

9. Creating database and tables if it's first run and inserting data into dimension tables. 

10. Inserting the data into fact table after checks.

11. Additional quality checks.

### Usage

Following are the steps for running and testing the data pipeline:

1. Make sure that PosgreSQL, Psycopg, Pandas and Pyspark are installed.

2. Run complete "Capstone_Project_Imroze.ipynb" Jupyter Notebook.

3. Run "etl.py" multiple times to do ETL on data for next months. The SAS data for full year 2016 is available in the workspace provided for project. 

4. An alternative to step 3 is installing AirFlow and running "dags/airflow_pipeline.py" and enabling dag through AirFlow UI. It is scheduled to run monthly, but can be scheduled to run after 15 minutes for testing, by replacing line 349 with commented line 348.

5. Run "test.ipynb" Jupyter notebook to view the results, data visualization and sample queries. It can be run anytime after first run of "Capstone_Project_Imroze.ipynb". 

