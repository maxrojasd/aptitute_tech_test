import sys
import os
curr_dir = os.path.dirname(os.path.abspath(__file__))
utils_folder = os.path.abspath(os.path.join(curr_dir, '..'))
sys.path.insert(1, utils_folder)
from utils.pyspark_utils import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import lit

# Creating initial SparkSession
spark = SparkSession \
        .builder \
        .appName('TLC_Trip_Records_Service_pyspark_unit_tests') \
        .getOrCreate()

# Unit Test per function
## filter_top_n()
def test_filter_top_n():
    # Create a test DataFrame
    schema = StructType(
        [
            StructField("trip_distance",
            DoubleType(), True)
            ]
        )
    data = [(1.0,), (2.0,), (3.0,), (4.0,), (5.0,)]
    test_df = spark.createDataFrame(data, schema)
    
    # Define top N %
    top_n = 0.1
    
    # Call the function being tested
    result_df = filter_top_n(test_df, top_n)
    
    # Check for expected results
    expected_result = [(5.0,)]
    expected_df = spark.createDataFrame(expected_result, schema)
    
    # Compare results
    result_data = result_df.collect()
    expected_data = expected_df.collect()
    
    assert result_data == expected_data


## Add more below accordingly

# Main function
if __name__ == '__main__':
    # Run first Unit test for get_timeframe_date()
    try:
        test_filter_top_n()
        print(f'Successfully ran test_filter_top_n_unit_test unit test')

    except Exception as e:
        print(f'Unit test test_filter_top_n_unit_test failed. Traceback below: {str(e)}')

    # Add more tests accordingly below..