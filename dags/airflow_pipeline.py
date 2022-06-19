from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
from os.path import exists
import glob
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType
from sql_queries import *
from create_tables import *
import time

def extract_monthly_csv():
    """
    - finds next month using monthlytimes table and loads SAS data for the new month into CSV
    """
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=immigdb user=student password=student")
        cur = conn.cursor()
    except:
        raise IOError("Database Connection cannot be established")
    try:
        cur.execute("SELECT monthlytime FROM monthlytimes",[])
    except:
        raise IOError("monthlytimes table not found")
    recs = cur.fetchone()
    if recs is None:
        raise ValueError("Data quality check failed. monthlytimes table has 0 rows")
    if len(recs) > 0:
        cur.execute("SELECT MAX(year) FROM monthlytimes",[])
        max_year = cur.fetchone()[0]
        print(max_year)
        cur.execute("SELECT MAX(month_num) FROM monthlytimes WHERE year = %s",[max_year])
        max_month = cur.fetchone()[0]
        print(max_month)
        prev_time = str(max_month)+'_'+str(max_year)
        print(prev_time)
        if max_month == 12:
            next_month = 1
            next_year = max_year + 1
        else:
            next_month = max_month + 1
            next_year = max_year
        next_year_abrv = str(next_year)[-2:]
        next_time = str(next_month)+'_'+str(next_year)
        monthDict = {1:'jan', 2:'feb', 3:'mar', 4:'apr', 5:'may', 6:'jun', \
            7:'jul', 8:'aug', 9:'sep', 10:'oct', 11:'nov', 12:'dec'}
        sas_path = "../../data/18-83510-I94-Data-{0}/i94_{1}{2}_sub.sas7bdat"\
        .format(next_year,monthDict[next_month],next_year_abrv)
        csv_path = "/home/workspace/immigration_csvs/F_{0}_{1}.csv".format(next_month,next_year)
        print(sas_path)
        print(csv_path)
        
        if not exists(csv_path):

            if exists(sas_path):
                print('converting SAS to CSV')
                immigCur_df = pd.read_sas(sas_path, 'sas7bdat', encoding="ISO-8859-1")
                immigCur_df.to_csv(csv_path,index = False)
                print('created : ',csv_path)
            else:
                raise IOError("SAS Files Not Found")
        else:
            print('CSV File already exists')
            
        month_name = monthDict[next_month]
        month_name = month_name[0].upper()+month_name[1:]
        cur.execute(months_table_insert,[next_month, month_name ] )
        cur.execute(years_table_insert,[next_year] )
        cur.execute(monthlytimes_table_insert,[ str(next_month)+'_'+str(next_year) ,next_year,next_month] )
    else:
        raise ValueError("Data quality check failed. monthlytimes table has 0 rows")
        
    conn.commit()
    conn.close()
    
    
def fact_table_etl():
    """
    - performs ETL for the latest month with data and adds new data into immigrationfacts table
    """
    print('Executing ETL')
    
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=immigdb user=student password=student")
        cur = conn.cursor()
    except:
        raise IOError("Database Connection cannot be established")
    try:
        cur.execute("SELECT monthlytime FROM monthlytimes",[])
    except:
        raise IOError("monthlytimes table not found")
    recs = cur.fetchone()
    if recs is None:
        raise ValueError("Data quality check failed. monthlytimes table has 0 rows")
    if len(recs) > 0:
        cur.execute("SELECT MAX(year) FROM monthlytimes",[])
        latest_year = cur.fetchone()[0]
        print(latest_year)
        cur.execute("SELECT MAX(month_num) FROM monthlytimes WHERE year = %s",[latest_year])
        latest_month = cur.fetchone()[0]
        print(latest_month)
        latest_time = str(latest_month)+'_'+str(latest_year)
        print(latest_time)
        
        ##################################################
        # variables used for quality checks
        allnull_row_check = True
        empty_check = True
        date_check = True
        cur.execute("SELECT COUNT(*) FROM immigrationfacts",[])
        init_rows_fact_table = int(cur.fetchone()[0])
        
        immi_schema = StructType([
            StructField("cicid", DoubleType()),
            StructField("i94yr", DoubleType()),
            StructField("i94mon", DoubleType()),
            StructField("i94cit", DoubleType()),
            StructField("i94res", DoubleType()),
            StructField("i94port", StringType()),    
            StructField("arrdate", DoubleType()),
            StructField("i94mode", DoubleType()),
            StructField("i94addr", StringType()),
            StructField("depdate", DoubleType()),
            StructField("i94bir", DoubleType()),
            StructField("i94visa", DoubleType()),
            StructField("count", DoubleType()),
            StructField("dtadfile", StringType()),
            StructField("visapost", StringType()),
            StructField("occup", StringType()),
            StructField("entdepa", StringType()),
            StructField("entdepd", StringType()),
            StructField("entdepu", StringType()),
            StructField("matflag", StringType()),
            StructField("biryear", DoubleType()),
            StructField("dtaddto", StringType()),
            StructField("gender", StringType()),
            StructField("insnum", StringType()),
            StructField("airline", StringType()),
            StructField("admnum", StringType()),
            StructField("fltno", StringType()),
            StructField("visatype", StringType()),
        ])
        
        cur.execute("SELECT * FROM airports",[])
        cols = []
        for elt in cur.description:
            cols.append( elt[0] )
        gaports = pd.DataFrame(data=cur.fetchall(), columns=cols )

        ga_portcodes = list(gaports.airport_code.unique())
        
        cur.execute("SELECT * FROM airlines",[])
        cols = []
        for elt in cur.description:
            cols.append( elt[0] )
        airlines_df = pd.DataFrame(data=cur.fetchall(), columns=cols )
        
        filename = "F_{month}_{year}.csv".format(month=str(latest_month),year=str(latest_year))
        print('Loading ',filename)
        
        spark = SparkSession.builder.master("local[1]").appName("EmpImmig").getOrCreate()
        immig_df = spark.read.csv('/home/workspace/immigration_csvs/'+filename,schema = immi_schema, header=True)
        
        immig_df = immig_df.drop("i94cit","i94res","i94mode","i94bir","i94addr","i94visa","count","depdate",\
                                 "dtadfile","visapost","occup","entdepa","entdepd","entdepu","matflag","dtaddto","insnum")
        
        # quality check for empty dataframe
        empty_check = immig_df.count() > 0
        
        # removing all rows where important fields are null
        immig_df = immig_df.filter(immig_df.visatype.isNotNull())
        immig_df = immig_df.filter(immig_df.i94port.isNotNull())
        immig_df = immig_df.filter(immig_df.i94yr.isNotNull())
        immig_df = immig_df.filter(immig_df.i94mon.isNotNull())
        immig_df = immig_df.filter(immig_df.admnum.isNotNull())
        
        # filtering to keep only E1, E2 and E3 visa type rows
        immig_df = immig_df.filter( (immig_df.visatype == "E1") | (immig_df.visatype  == "E2") | (immig_df.visatype  == "E3") )
        
        # filtering to keep only rows with Georgia airports
        immig_df = immig_df.filter( immig_df.i94port.isin(ga_portcodes) )
        
        print(immig_df.count())
        
        immig_df = immig_df.dropDuplicates(["cicid"])
        
        immig_df = immig_df.withColumn("i94yr", col("i94yr").cast(IntegerType()) )
        immig_df = immig_df.withColumn("i94mon", col("i94mon").cast(IntegerType()) )
        
        # quality check for date
        date_check = ( immig_df.filter(  (immig_df.i94yr != latest_year) | (immig_df.i94mon != latest_month) )  ).count() == 0
        
        # filling missing/NaN values of gender with U (unknown)
        immig_df = immig_df.na.fill(value='U',subset=["gender"])
        
        # filling missing airline values with unknown
        immig_df = immig_df.na.fill(value='Unknown',subset=["airline"])
        
        print(immig_df.count())
        
        # user defined function (udf) to convert SAS date to day of month integer
        @udf(returnType=IntegerType())
        def sasdate_day(date):
            pd_timestamp = pd.to_timedelta(date,unit='D')+pd.Timestamp('1960-1-1')
            return int( pd_timestamp.strftime('%d') )
        
        immig_df = immig_df.withColumn("day", sasdate_day(col("arrdate")))
        immig_df = immig_df.drop("arrdate")
        
        # user defined function (udf) to remove decimal in admnum string
        @udf(returnType=StringType())
        def parse_admnum(admnum):
            if '.' in admnum:
                return admnum[ : admnum.find('.') ]
            else:
                return admnum
            
        immig_df = immig_df.withColumn("cicid", col("cicid").cast(IntegerType()) )
        immig_df = immig_df.withColumn("biryear", col("biryear").cast(IntegerType()) )
        immig_df = immig_df.withColumn("admnum", parse_admnum(col("admnum")) )
        immig_df = immig_df.withColumn("age", (latest_year - col("biryear")) )
        
        # user defined function (udf) to convert age to 5 age-groups
        @udf(returnType=IntegerType())
        def get_agegroup(age):
            if age <= 25:
                return 1
            if age > 25 and age <= 35:
                return 2
            if age > 35 and age <= 45:
                return 3
            if age > 45 and age <= 59:
                return 4
            if age >= 60:
                return 5
            return 0
        
        immig_df = immig_df.withColumn("agegroup", get_agegroup(col("age")))
        immig_df = immig_df.withColumn("month_year", lit(str(latest_month)+'_'+str(latest_year))  )
        
        gaports_spark = spark.createDataFrame(gaports) 
        
        fact_df = immig_df.join( gaports_spark, immig_df.i94port == gaports_spark.airport_code, how = 'inner' )\
                            .select( col("cicid").alias("CICID"), col("i94yr").alias("Year"),\
                                    col("i94mon").alias("Month"), col("day").alias("Day"),\
                                    col("month_year").alias("Monthly_Time"),\
                                    col("visatype").alias("Visa_Type"), col("gender").alias("Gender"),\
                                    col("age").alias("Age"), col("agegroup").alias("Age_Group"),\
                                    col("airport_code").alias("Airport_Code"), col("municipality").alias("Airport_Municipality"),\
                                    col("admnum").alias("Adm_Number"), col("airline").alias("Airline_Code"),\
                                    col("fltno").alias("Flight_Number") )
        
        airlines_spark = spark.createDataFrame(airlines_df) 
        
        fact_df = fact_df.join(airlines_spark,fact_df.Airline_Code == airlines_spark.airline_code, how='left')
        
        fact_df = fact_df.drop("airline_name")
        fact_df = fact_df.drop("airline_country")
        
        fact_df.show(n=5)
        
        fact_dfpd = fact_df.toPandas() 
        
        for colu in list(fact_dfpd.columns):
            if not fact_dfpd[colu].notnull().any():
                print('All Null rows in ',colu)
                allnull_row_check = False
                break
                
        if allnull_row_check:
            for index, row in fact_dfpd.iterrows():
                vals = [row.CICID,row.Year,row.Month,row.Day,row.Monthly_Time,row.Visa_Type,row.Gender,\
                       row.Age,row.Age_Group,row.Airport_Code,row.Airport_Municipality,row.Adm_Number,\
                       row.Airline_Code,row.Flight_Number]
                cur.execute(immigrationfacts_table_insert,vals)
                
        # checking that all dimension tables have been created and populated
        dim_tables = ['visatypes','genders','agegroups','airports','municipalities',\
                      'years','months','monthlytimes','airlines']
        for dim_tbl in dim_tables:
            try:
                cur.execute("SELECT COUNT(*) FROM "+dim_tbl,[])
                res = cur.fetchone()[0]
                if res==0:
                    raise ValueError("Data quality check failed. No rows in "+dim_tbl)
                print(dim_tbl,' table found, having',res,' rows')   
            except:
                raise IOError( dim_tbl+" table not found")
                
        #implemented during ETL above
        # checking if data loaded was not empty
        assert empty_check, "Failed check for loading csv data as PySpark DataFrame"
        print('Passed check for loading csv data as PySpark DataFrame')
        
        #implemented during ETL above
        # checking if data being added is of correct time period
        assert date_check, 'Failed check for time period in loaded data'
        print('Passed check for time period in loaded data')
        
        #implemented during ETL above
        #Full Columns are Null if data for column is not loaded correctly in Spark DataFrame due to mismatch with schema types
        assert allnull_row_check, 'Failed check for no fully Null column in new portion of fact table'
        print('Passed check for no fully Null column in new portion of fact table')
        
        # implementing quality check for new records being added in face table, after checking that table exists
        try:
            cur.execute("SELECT COUNT(*) FROM immigrationfacts",[])
            print('immigrationfacts table found')
        except:
            raise IOError("immigrationfacts table not found")
        new_rows_fact_table = cur.fetchone()[0]
        print('Row Count Before: ',init_rows_fact_table)
        print('Row Count After: ',new_rows_fact_table)
        print('New Rows Added: ',new_rows_fact_table-init_rows_fact_table)
        assert new_rows_fact_table > init_rows_fact_table, 'Failed check for new records added in fact table'
        print('Passed check for new records added in fact table')
        
        cur.execute("SELECT * FROM immigrationfacts LIMIT 1",[])
        no_cols = len(cur.fetchone())
        print(no_cols,'columns in immigrationfacts table')
        assert no_cols==14, 'Failed check for number of columns in immigrationfacts'
        print('Passed check for number of columns in immigrationfacts')
        
        print('ETL Completed')
        
        ##################################################
        
    else:
        raise ValueError("Data quality check failed. monthlytimes table has 0 rows")
        
    conn.commit()
    conn.close()


default_args = {
'owner': 'imroze',
'start_date': datetime(2022, 6, 17)
}

dag = DAG(
    dag_id='immigration_dag',
    catchup=False,
    default_args=default_args,
    #schedule_interval= '*/15 * * * *')
    schedule_interval= '@monthly') 

task1 = PythonOperator(task_id='ingestion_task',
                       python_callable=extract_monthly_csv,
                       dag=dag)

task2 = PythonOperator(task_id='etl_task',
                       python_callable=fact_table_etl,
                       dag=dag)

task1 >> task2