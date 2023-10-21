from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
from selenium import webdriver
from bs4 import BeautifulSoup
import requests
import os
import pandas as pd
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, format_number
# Functions

def get_input_date(timeframe):
    while True:
        input_date = input('(MM-YYYY): ')
        try:
            # Transform date to page source format
            # Split date
            trans_date = datetime.strptime(input_date, '%m-%Y')
            year = trans_date.year
            # Get month as text
            month_str = trans_date.strftime('%B')
            # Get month as number
            month_int = int(trans_date.strftime('%m'))
            print(f'{timeframe} date is {month_str} of {year}')
            print(month_int)
            return year, month_int
        except ValueError:
            print('Date in incorrect format, please try again with format (MM-YYYY): ')
            continue
# Function to create timeframe for the period selected
def get_timeframe(init_year, init_month,final_year, final_month):
    timeframe = {}
    # Create datatime objects
    init_date = datetime(init_year, init_month, 1)
    final_date = datetime(final_year, final_month, 1)

    # Generate the timeframe elements
    while init_date <= final_date:
        year = init_date.year
        month = init_date.strftime('%m')

        # Add the elements to the corresponding dict
        if year in timeframe:
            timeframe[year].append(month)
        else:
            timeframe[year] = [month]

        # Monthly counter
        init_date += relativedelta(months=1)

    return timeframe

    
# Input to ask for the date range
## Initial Date
# print('Please enter the initial date in the format below')
# init_year, init_month = get_input_date('initial')
# ## Final Date
# print('Please enter the final date in the format below')
# final_year, final_month = get_input_date('final')
# Get the timeframe for the data to be extracted
timeframe = get_timeframe(2022, 1,2022, 3)
# print(timeframe)
# Configure the Selenium web driver
driver = webdriver.Chrome()
url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page' 
# # Get page source to scrap data
driver.get(url)
page_source = driver.page_source
# print(page_source)

# Close browser
driver.close()

## OPTIONAL: Save HTML file for exploration
# file_path = 'page_source_raw.html'
# with open(file_path, 'w') as html_file:
#     html_file.write(page_source)

# Load HTML file for exploring
# file_path = 'page_source_raw.html'
# with open(file_path, 'r', encoding='utf-8') as file:
#     page_source = file.read()
# print(page_source)

# Get parquet files with BeautifulSoup for the period we need
soup = BeautifulSoup(page_source, 'html.parser')

## Find all the parquet files for the timeframe

#Create curated list object
curated_urls_per_year = {}
for year, months_list in timeframe.items():
    
    # Create list for the urls fetched
    curated_urls_per_year[year] = []

    # Get the list of all parquet files per year and month
    css_tag = 'faq'+ str(year)
    elements_per_year = soup.find(id = css_tag)

    # Get table with data related to specific year
    year_table = elements_per_year.find('table')

    # Find all the elements per month itearting year_table
    yearly_trip_record_file_url_list = []

    # Iterate through the rows of the table
    for yearly_trip_record in year_table.find_all('li'):
        yearly_trip_record_file_url = str(yearly_trip_record.find('a').get('href'))

        # Check if url is part of the months_list
        if yearly_trip_record_file_url[-10:-8] in months_list:
            yearly_trip_record_file_url_list.append(yearly_trip_record_file_url)

        # Filter only trip records for desire line
        # This variable can be changed for any other line in case is needed (can go to a config file to parametize)
        type_of_TLC = 'yellow'
        filtered_urls = [
            url
            for url in yearly_trip_record_file_url_list
            if type_of_TLC in url.lower()
        ]

    # Append filtered urls to Object   
    curated_urls_per_year[year].append(filtered_urls)
    print(filtered_urls)

# Create folder to download parquet files
## This variable can be modified and moved to a config file
download_folder = 'raw_data'
if not os.path.exists(download_folder):
    os.mkdir(download_folder)

# Download all the desire files to the download_folder
for url in filtered_urls:
    if any(month in url for month in months_list):
        file_name = url.split('/')[-1]
        response = requests.get(url)
        if response.status_code == 200:
            with open(os.path.join(download_folder, file_name), 'wb') as f:
                f.write(response.content)
            print(f'{file_name} downloaded in {download_folder}')
        else:
            print(f'{file_name} download failed')

print(f'All parquet files are download in {download_folder}')

# Creating SparkSession
spark = SparkSession \
        .builder \
        .appName('TLC_Trip_Records_Service') \
        .getOrCreate()

# Reading Parquet files to a dataframe
df = spark.read.parquet(download_folder)
# Count the number of rows in the DataFrame
records_count = df.count()
print(f'Total number of TLC records: {records_count}')

# Filtering top 10% trips
## This variable can be modified
top_n = 0.1
top_10_num = int(df.count()*top_n)
print(f'Now filtered top {top_n*100}% of TLC records. Number of records :{top_10_num}')
# Order elements and get top N % 
top_n_df = df.orderBy(col('trip_distance').desc()) \
                        .limit(top_10_num)

# Replace None values with 0.0
total_amount_not_null_df = top_n_df.withColumn('total_amount', col('total_amount').cast('float'))
total_amount_not_null_df = total_amount_not_null_df.na.fill(0.0, ['total_amount'])

# Format total_amount numbers to have 2 decimals
top_n_df = top_n_df.withColumn('total_amount',
                                     format_number(col('total_amount'), 2)
                                     )

# Calculating total amount based on values in relevant columns for the total sum.
# Does not include cash tips as in the data Dictionary is specified this way
top_n_df = top_n_df.withColumn('total_amount_sum',
                                      format_number(
                                          round(col('fare_amount')+col('extra')
                                            +col('tolls_amount')+col('improvement_surcharge')+col('mta_tax')
                                     , 2)
                                     , 2)
                                     )

# Add column to check if calculation is correct
total_sum_df = top_n_df.withColumn('is_sum_correct',
                                      when(col('total_amount') == col('total_amount_sum'), True).otherwise(False)
                                      )

# Converting total_amount column to the positive sum of the variables for the formula to calculate total amount


formated_total_amount_df = total_sum_df.withColumn('total_amount',
                                     when(col('total_amount') != col('total_amount_sum'),
                                          col('total_amount_sum')*-1
                                          )\
                                          .otherwise(col('total_amount')*-1)
                                     )

# Updating total_amount column

updated_df = formated_total_amount_df.withColumn('total_amount',col('total_amount_sum'))
updated_df = updated_df.drop(*['total_amount_sum','is_sum_correct'])

# Save new dataframe as parquet in output_folder
output_folder = "/output/top_10_yellow_line_TLC_records.parquet"

# Save the DataFrame as a Parquet file
updated_df.write\
          .mode("overwrite")\
          .parquet(output_folder)

updated_df.show()

# Stop spark
spark.stop()
