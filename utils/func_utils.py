from pyspark.sql import SparkSession


def create_table_schema(df, redshift_table_name):
    # Create a temp view of the df
    df.createOrReplaceTempView("temp_view")

    # Get the schema for the
    schema_info = spark.sql("DESCRIBE temp_view")

    # Create columns with datatype list
    columns = [
        f"{row['col_name']} {row['data_type']}" for row in schema_info.collect()]

    # Create initial sql query
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {redshift_table_name} (" + ",".join(
        columns) + ");"

    # Fixing format for some variables in the string
    create_table_sql = create_table_sql.replace('array<double>', 'VARCHAR(255)')\
        .replace('string', 'VARCHAR(255)')\
        .replace('double', 'FLOAT')

    return create_table_sql.replace('array<double>', 'VARCHAR(255)').replace('string', 'VARCHAR(255)')


def create_insert_statements(df, redshift_table_name, target_chunk_size_bytes=90 * 1024):
    # Get column names
    columns = df.columns

    # Create initial sql query
    insert_sql = f"INSERT INTO {redshift_table_name} ({', '.join(columns)}) VALUES "

    # Generate placeholders for values in the INSERT statement
    value_placeholders = '(' + ', '.join(['%s' for _ in columns]) + ')'

    # Collect data from df
    data = df.collect()

    insert_statements = []
    chunk = [insert_sql]
    size = len(insert_sql.encode('utf-8'))

    for row in data:
        # Create a single sql query
        value_to_insert = value_placeholders % tuple(row)

        # Calculate the size of the current INSERT statement in bytes
        current_insert_size = len(value_to_insert.encode('utf-8'))

        # Check if chunk exceed the size
        if size + current_insert_size > target_chunk_size_bytes:
            insert_statements.append(chunk)
            chunk = [insert_sql]
            size = len(insert_sql.encode('utf-8'))

        chunk.append(value_to_insert)
        size += current_insert_size

    # Add all extra to last chunk
    if len(chunk) > 1:
        insert_statements.append(chunk)

    return insert_statements

def extract_values(record):
    # Extract values from Redshift's response
    values = []
    for item in record:
        for key in item:
            if key.endswith('Value'):
                values.append(item[key])
            elif key == 'isNull' and item[key]:
                values.append(None)
    return values

def get_df_from_redshift_query(query_id):
    
    # Check Results
    redshift_data_client.get_statement_result(Id=query_id)
    # Get the results of the query
    response = redshift_data_client.get_statement_result(Id=query_id)

    # Extract the column names from the metadata
    columns = [metadata['name'] for metadata in response['ColumnMetadata']]

    # Extract the data from the records
    data = [extract_values(record) for record in response['Records']]

    # Define Schema for Pyspark dataframe
    schema = StructType([StructField(name, StringType(), True) for name in columns])

    # Create a PySpark DataFrame
    new_redshift_table_df = spark.createDataFrame(data, schema=schema)

    return new_redshift_table_df

def execute_redshift_query(cluster, db, query):
  response_query = redshift_data_client.execute_statement(
    ClusterIdentifier= cluster,
    Database=db,
    Sql= query
  )
  return response_query