import pandas as pd
import requests
import json
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, second

# Getting openAQ Data from API (Need to get API Online)
openaq_res = requests.get(
    'https://api.openaq.org/v2/locations', headers={'X-API-Key': ''})

# get JSON File from requests
openaq_json = openaq_res.json()

# Convert JSON to pandas df
df = pd.DataFrame(openaq_json['results'])

# Rename columns
df = df.rename(columns={'lastUpdated': 'lastUpdated_record',
                        'firstUpdated': 'firstUpdated_record',
                        'id': 'id_record'})

# Expand JSON columns


def expand_df(df, column):
    return df.explode(column).reset_index(drop=True)


# Apply function to df
# df = df['parameters'].apply(normalize_column)
# pd.DataFrame(df['parameters'][0])
df = expand_df(df, 'parameters')
df = expand_df(df, 'manufacturers')
columns_to_replace = [col for col in df.columns if df[col].isna().all()]

# Replace NaN values with the defined string
for col in columns_to_replace:
    df[col] = df[col].fillna('NULL')

# Normalize dataframe
df2_normalized_paramenters = pd.json_normalize(df['parameters'])
df2_normalized_manufacturers = pd.json_normalize(df['manufacturers'])

# Concat all the values
df_concat = pd.concat([df, df2_normalized_paramenters,
                      df2_normalized_manufacturers], axis=1)

# drop columns that where normalized
df_concat = df_concat.drop(columns=['parameters', 'manufacturers'])


# Create a Spark configuration
# Note: change parameters accordingly
conf = SparkConf().setAppName("OpenAQ_Service") \
                  .set("spark.hadoop.fs.s3a.access.key", "") \
                  .set("spark.hadoop.fs.s3a.secret.key", "") \
                  .set("spark.hadoop.fs.s3a.impl", "")

# Initialize a Spark session with the configuration
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Create pyspark dataframe
pyspark_df = spark.createDataFrame(df_concat)

# Split the 'coordinates' struct into two separate columns
pyspark_df = pyspark_df.withColumn("latitude", col("coordinates.latitude"))
pyspark_df = pyspark_df.withColumn("longitude", col("coordinates.longitude"))

# Drop the original 'coordinates' column
coordinates_fixed_df = pyspark_df.drop("coordinates")

dates_updated_df = coordinates_fixed_df.withColumn("firstUpdatedYear_record", year(col("firstUpdated_record"))) \
    .withColumn("firstUpdatedMonth_record", month(col("firstUpdated_record"))) \
    .withColumn("firstUpdatedDay_record", dayofmonth(col("firstUpdated_record"))) \
    .withColumn("firstUpdatedHour_record", hour(col("firstUpdated_record"))) \
    .withColumn("firstUpdatedMinute_record", minute(col("firstUpdated_record"))) \
    .withColumn("firstUpdatedSecond_record", second(col("firstUpdated_record"))) \
    .withColumn("lastUpdatedYea_record", year(col("lastUpdated_record"))) \
    .withColumn("lastUpdatedMonth_record", month(col("lastUpdated_record"))) \
    .withColumn("lastUpdatedDay_record", dayofmonth(col("lastUpdated_record"))) \
    .withColumn("lastUpdatedHour_record", hour(col("lastUpdated_record"))) \
    .withColumn("lastUpdatedMinute_record", minute(col("lastUpdated_record"))) \
    .withColumn("lastUpdatedSecond_record", second(col("lastUpdated_record"))) \
    .dropna() \
    .dropDuplicates()

# Reading Parquet files to a dataframe
output_filepath = 'output/output.parquet'
dates_updated_df = dates_updated_df.write\
    .mode("overwrite")\
    .parquet(output_filepath)

spark.stop()
