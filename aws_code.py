from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import split
from utils.func_utils import *
import boto3
import os
from dotenv import load_dotenv
import time
import s3fs
from datetime import datetime 

# Load .env file
load_dotenv()

# Declare variables from environment
# Note: change parameters accordingly in .env file 
aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')
redshift_table_name = os.getenv('redshift_table_name')
service_name_s3 = os.getenv('service_name_s3')
service_name_redshift = os.getenv('service_name_redshift')
region_name = os.getenv('region_name')
cluster = os.getenv('cluster')
db = os.getenv('db')
redshift_table_name = os.getenv('redshift_table_name')
s3_bucket = os.getenv('s3_bucket')

# Creating SparkSession
## Create a Spark configuration
conf = SparkConf().setAppName('OpenAQ_Service') \
                  .set('spark.hadoop.fs.s3a.access.key', aws_secret_access_key) \
                  .set('spark.hadoop.fs.s3a.secret.key', aws_access_key_id) \
                  .set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

# Initialize a Spark session with the configuration
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Reading files to a dataframe
input_folder = 'output'

# Read Parquet
# df = spark.read.parquet(input_folder)

# Read CSV
## Change with any location accordingly
input_filepath = '2017-08-19.csv'
df = spark.read.csv(input_filepath, header=False, inferSchema=True)

# Fixing Columns
df_columns_fixed = df.withColumnRenamed('_c0', 'location') \
    .withColumnRenamed('_c1', 'value') \
    .withColumnRenamed('_c2', 'unit') \
    .withColumnRenamed('_c3', 'pollutant') \
    .withColumnRenamed('_c4', 'country') \
    .withColumnRenamed('_c5', 'city') \
    .withColumnRenamed('_c6', 'name') \
    .withColumnRenamed('_c7', 'timestamp') \
    .withColumnRenamed('_c11', 'longitude') \
    .withColumnRenamed('_c12', 'latitude') \
    .drop('_c8').drop('_c9').drop('_c10').drop('_c13').drop('_c14')

## Comments for deleted columns
    # '_c8' # Duplicated column (timestamp)
    # '_c9'  # Irrelevant, always government
    # '_c10'  # Irrelevant, always false
    # '_c13' # Number of hours of measurement
    # '_c14' # unit for _c13

# Fix date & time 

splitted_col = split(df_columns_fixed['timestamp'], ' ')
df_date_time_separated = df_columns_fixed.withColumn('date', splitted_col.getItem(0)) \
       .withColumn('time', splitted_col.getItem(1))
splitted_date = split(df_date_time_separated['date'], '-')
splitted_time = split(df_date_time_separated['time'], ':')
df_date_time_fixed = df_date_time_separated.withColumn('day', splitted_date.getItem(2)) \
                                           .withColumn('month', splitted_date.getItem(1)) \
                                           .withColumn('year', splitted_date.getItem(0)) \
                                           .withColumn('hour', splitted_time.getItem(0)) \
                                           .withColumn('minute', splitted_time.getItem(1)) \
                                           .withColumn('second', splitted_time.getItem(2))


# Create table schema query
create_table_schema_query = create_table_schema(df_date_time_fixed, redshift_table_name)

# Run Redshift query
response_data = execute_redshift_query(cluster, db, create_table_schema_query)

# Get query Id
query_id = response_data['Id']

print(f'The following query: \n {create_table_schema_query} \n is running in redshift with job Id: \n {query_id} \n')
time.sleep(2)

# Check if Table was succesfully created
sql_query = f'''
select * from public.{redshift_table_name}
'''
## Run Redshift query
response_data = execute_redshift_query(cluster, db, sql_query)
if response_data:
  print(f'table {redshift_table_name} created succesfully')
else:
  print('table failed to be created, please check log')

## Get new query Id
query_id = response_data['Id']

time.sleep(1)
## Get results Pyspark Dataframe
result_redshift_table_df = get_df_from_redshift_query(query_id)

## Get number of records
print(f'Total number of Records: {result_redshift_table_df.count()}')

# Display results
result_redshift_table_df.show()

# Convert dataframe to pandas
df_date_time_fixed_pd =df_date_time_fixed.toPandas()

csv_filepath = f'{redshift_table_name}.csv'

# Save .csv in case is needed
# df_date_time_fixed_pd.to_csv(csv_filepath)

# Open a file on S3
s3_fs = s3fs.S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)

# Write the DataFrame to Parquet format in s3 folder
## Change this variable accordingly
s3_key = f'{redshift_table_name}/{redshift_table_name}.parquet'
with s3_fs.open(f's3a://{s3_bucket}/{s3_key}', 'wb') as f:
    
    df_date_time_fixed_pd.to_parquet(f) 
print(f'file {s3_key} saved in S3 bucket: {s3_bucket}')

# Stop spark
spark.stop()
print('SparkSession over.')