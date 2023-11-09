# TLC_Trip_Records_Service APP
Script to get the top N records from a specific TLC line and fix the total amount of the trips.

## Pre-requirements
In order to run the script, make sure to have the following:
- Spark
- Selenium
- BeautifulSoup
- Request
- pytest

## How-to-run
Just run the main.py script

# Unit Test
I've created the structure to implement unit tests for all the scripts and functions created. this can be found in unit_tests.py script and in unit_tests folder.

## The approach

As the requirements was to dinamically download records from a specific timefrime given by the user, the best approach to do this I could think of, was to use Selenium and beautifulSoup, therefore, I've created the 'tlc_scrapper.py' script to download the specific parquet file to a desire folder.

After having the download records, to understand and explore the data, I've created a support_notebook (you can find it in the root folder) to do the 'pyspark_code.py' and perform some EDA.

From this analysis, I could noticed some data issues like negative or 0 values for the total amount, therefore, I've fixed them, also included handling null values. 

I believe that there are many pain points in the data that affects the data quality, so, fixing the total_amount column to make sure the values are correct would involve more in-depth analysis to identify the root-cause for the errors. An example of this is the fact that there are 'Payment_type' that are not included in the numeric code in the [Yellow Trips Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) provided by the TLC Trip Record Data.

Given the nature of the data and that many columns affect the total_amount values, I prefered to create a custom orchestrator to run custom tests, because, for example if the data needs to be check by different vendors, payment type or geography it might have different limitants, for example, if a trip is from JFK it will have an extra fee that will affect the total amount directly, so if the fee is not charged for a trip in the area, it might pass some basic validations, but we will now that the actual value is incorrect.


