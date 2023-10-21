import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, format_number


# Functions


# Filter top N records
def filter_top_n(df, top_n):
    top_10_num = int(df.count() * top_n)
    top_n_df = df.orderBy(col('trip_distance').desc()).limit(top_10_num)
    return top_n_df

# Replace None values
def replace_none_values(df):
    total_amount_not_null_df = df.withColumn('total_amount', col('total_amount').cast('float'))
    total_amount_not_null_df = total_amount_not_null_df.na.fill(0.0, ['total_amount'])
    return total_amount_not_null_df

# Total_amount formatter
def format_total_amount(df):
    top_n_df = df.withColumn('total_amount', format_number(col('total_amount'), 2))
    return top_n_df

# total_amount calculator
def calculate_total_amount(df):
    top_n_df = df.withColumn('total_amount_sum', format_number(round(
        col('fare_amount') + col('extra') + col('tolls_amount') + col('improvement_surcharge') + col('mta_tax'), 2), 2))
    return top_n_df

# total_amount validator
def add_total_amount_validation_column(df):
    total_sum_df = df.withColumn('is_sum_correct',
                                when(col('total_amount') == col('total_amount_sum'), True).otherwise(False))
    return total_sum_df

# total_amount negative to positive number converter
def convert_total_amount_to_positive(df):
    formated_total_amount_df = df.withColumn('total_amount', when(col('total_amount') != col('total_amount_sum'),
                                                                 col('total_amount_sum') * -1).otherwise(col('total_amount') * -1))
    return formated_total_amount_df

# Delete irrelevant columns
def update_total_amount_column(df):
    updated_df = df.withColumn('total_amount', col('total_amount_sum')).drop(*['total_amount_sum', 'is_sum_correct'])
    return updated_df