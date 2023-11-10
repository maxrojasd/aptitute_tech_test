from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType
from utils.func_utils import *
import boto3

# Creating SparkSession
# Create a Spark configuration
# Note: change parameters accordingly
conf = SparkConf().setAppName("OpenAQ_Service") \
                  .set("spark.hadoop.fs.s3a.access.key", "") \
                  .set("spark.hadoop.fs.s3a.secret.key", "") \
                  .set("spark.hadoop.fs.s3a.impl", "")

# Initialize a Spark session with the configuration
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Reading Parquet files to a dataframe
input_folder = 'output'
df = spark.read.parquet(input_folder)

# Count the number of rows in the DataFrame
records_count = df.count()
print(f'Total number of records: {records_count}')

# Connect to S3 in AWS (Replace variables accordingly)
s3 = boto3.resource(
    service_name='s3',
    region_name='',
    aws_access_key_id='',
    aws_secret_access_key=''
)

# Convert parquet file to CSV for Redshift perfomance
s3_filename = 'output.csv'
df.drop('bounds').toPandas().to_csv()

# Upload new file to S3 (change bucket Name)
s3.Bucket('').upload_file(Filename=s3_filename, Key=s3_filename)

# Create SQL Query to upload tables to Redshift
create_table_schema_statement = create_table_schema(df, 'openaq_table')

# Create SQL Query to update tables in Redshift
insert_sql= create_insert_statements(df, 'openaq_table')

# Connect to RedShift
redshift_data_client = boto3.client('redshift-data',
                              aws_access_key_id = '',
                              aws_secret_access_key = ''
                               )
# Create new Table
response = redshift_data_client.execute_statement(
    ClusterIdentifier='redshift-cluster-1',
    Database='dev',
    Sql= create_table_schema_statement
)

# Check freshly new created table

response_check_table = redshift_data_client.execute_statement(
    ClusterIdentifier='redshift-cluster-1',
    Database='dev',
    Sql='select * from test_4'
)
# Check JobId
query_id = response_check_table['Id']
print(query_id)

# Get statement Results
redshift_data_client.get_statement_result(Id=query_id)
# Get the results of the query
response = redshift_data_client.get_statement_result(Id=query_id)

# Extract the column names from the metadata
columns = [metadata['name'] for metadata in response['ColumnMetadata']]

# Extract the data from the records
data = [extract_values(record) for record in response['Records']]

# Create schema for df
schema = StructType([StructField(name, StringType(), True) for name in df.columns])

# Create df
df = spark.createDataFrame(data, schema=schema)

# Display df to check new dataframe inserted
df.show()

# Stop spark
spark.stop()
print('SparkSession over.')
