
import os
from pyspark.sql import SparkSession
from utils.pyspark_utils import *

# Creating SparkSession
spark = SparkSession \
        .builder \
        .appName('TLC_Trip_Records_Service') \
        .getOrCreate()
input_folder = 'raw_data'

# Reading Parquet files to a dataframe
input_folder = 'sample_data'
df = spark.read.parquet(input_folder)
# Count the number of rows in the DataFrame
records_count = df.count()
print(f'Total number of TLC records: {records_count}')

# Filtering top 10% trips
## This variable can be modified
top_n = 0.1
top_10_num = int(df.count()*top_n)
print(f'Now filtered top {top_n*100}% of TLC records. Number of records :{top_10_num}')

# Order elements and get top N %
top_n_df = filter_top_n(df, top_n)

# Replace None values with 0.0
total_amount_not_null_df = replace_none_values(top_n_df)
print('None values replaced.')

# Format total_amount numbers to have 2 decimals
total_amount_not_null_df = format_total_amount(total_amount_not_null_df)
print('Formatted total_amount with 2 decimals.')

# Calculating total_amount based on values in relevant columns for the total sum.
# Does not include cash tips as in the data Dictionary is specified this way
total_amount_not_null_df = calculate_total_amount(total_amount_not_null_df)
print('total_amount correction column calculated.')

# Add column to check if calculation is correct
total_amount_not_null_df = add_total_amount_validation_column(total_amount_not_null_df)
print('total_amount validation column created.')

# Converting total_amount column to the positive sum of the variables for the formula to calculate total_amount
total_amount_not_null_df = convert_total_amount_to_positive(total_amount_not_null_df)
print('total_amount values changed to positive.')

# Updating total_amount column
updated_df = update_total_amount_column(total_amount_not_null_df)
print('total_amount new values updated accordingly.')

# Create folder to save parquet file
## This variable can be modified and moved to a config file
output_folder = '/output/top_10_yellow_line_TLC_records.parquet'
if not os.path.exists(output_folder):
    os.mkdir(output_folder)
    print(f'folder {output_folder} created.')

# Save the DataFrame as a Parquet file
updated_df.write\
          .mode("overwrite")\
          .parquet(output_folder)
print('Parquet file saved.')


# Display output dataframe
updated_df.show()

# Stop spark
spark.stop()
print('SparkSession over.')
