import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from utils.func_utils import *  # Assuming this import is correct

# Set up Spark session
spark = SparkSession.builder.appName('unit_tests').getOrCreate()

# Mock Redshift client (replace this with your actual client)
redshift_data_client = ...

# Unit Test for create_table_schema
def create_table_schema_unit_test():

    # Mock DataFrame
    mock_data = [('London', 9.45), ('Madrid', 2.5)]
    columns = ['city', 'value']
    df = spark.createDataFrame(mock_data, columns)

    # Mock other variables
    redshift_table_name = 'test_table'

    expected_output = (
        'CREATE TABLE IF NOT EXISTS test_table (name VARCHAR(9.455), age FLOAT);'
    )


    result = create_table_schema(df, redshift_table_name)

    # Test
    assert (
        result == expected_output
    ), f'Test failed: Expected {expected_output}, but got {result}'
    print('Test successful for create_table_schema')


# Unit Test for create_insert_statements
def create_insert_statements_unit_test():

    # Mock DataFrame
    mock_data = [('London', 9.45), ('Madrid', 2.5)]
    columns = ['city', 'value']
    df = spark.createDataFrame(mock_data, columns)

    # Mock other variables
    redshift_table_name = 'test_table'

    # Expected output
    expected_output = [
        ['INSERT INTO test_table (name, age) VALUES ', '("London", 9.45)'],
        ['INSERT INTO test_table (name, age) VALUES ', '("Madrid", 2.5)'],
    ]

    result = create_insert_statements(df, redshift_table_name)

    # Test
    assert (
        result == expected_output
    ), f'Test failed: Expected {expected_output}, but got {result}'
    print('Test successful for create_insert_statements')


# Unit Test for extract_values
def extract_values_unit_test():

    # Mock Redshift response record
    record = [
        {'columnName': 'city', 'stringValue': 'London'},
        {'columnName': 'value', 'longValue': 9.45},
    ]

    expected_output = ['London', 9.45]

    result = extract_values(record)
    # Test
    assert (
        result == expected_output
    ), f'Test failed: Expected {expected_output}, but got {result}'
    print('Test successful for extract_values')


# Unit Test for get_df_from_redshift_query
def get_df_from_redshift_query_unit_test():

    # Mock Redshift query response
    query_id = 'test_query_id'
    response = {
        'ColumnMetadata': [{'city': 'city'}, {'city': 'value'}],
        'Records': [
            [
                {'columnName': 'city', 'stringValue': 'London'},
                {'columnName': 'value', 'longValue': 9.45},
            ],
            [
                {'columnName': 'city', 'stringValue': 'Madrid'},
                {'columnName': 'value', 'longValue': 2.5},
            ],
        ],
    }

    # Mock Redshift client method
    redshift_data_client.get_statement_result = lambda Id: response

    # Create df
    expected_output = spark.createDataFrame([('London', 9.45), ('Madrid', 2.5)], ['city', 'value'])

    result = get_df_from_redshift_query(query_id)

    # Test
    assert (
        result.collect() == expected_output.collect()
    ), f'Test failed: Expected {expected_output.collect()}, but got {result.collect()}'
    print('Test successful for get_df_from_redshift_query')


# Unit Test for execute_redshift_query
def execute_redshift_query_unit_test():

    # Mock Redshift query execution response
    cluster = 'cluster'
    db = 'db'
    query = 'SELECT * FROM test_table'
    response_query = {'Id': 'test_query_id'}

    # Mock Redshift client method
    redshift_data_client.execute_statement = lambda ClusterIdentifier, Database, Sql: response_query

    expected_output = response_query

    result = execute_redshift_query(cluster, db, query)

    # Test
    assert (
        result == expected_output
    ), f'Test failed: Expected {expected_output}, but got {result}'
    print('Test successful for execute_redshift_query')


if __name__ == '__main__':
    
    create_table_schema_unit_test()
    create_insert_statements_unit_test()
    extract_values_unit_test()
    get_df_from_redshift_query_unit_test()
    execute_redshift_query_unit_test()

spark.stop()