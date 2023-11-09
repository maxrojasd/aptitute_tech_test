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

# Query 1
result = spark.sql("""
WITH Percentile_ct AS (
  SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY average) AS 90th_percentile
  FROM split_date_df
)
SELECT distinct city
FROM plit_date_df
WHERE average < (SELECT 90th_percentile FROM Percentile_ct);
""")
result.show()


# query 2
result = spark.sql(
    """SELECT distinct name, average
      FROM split_date_df
        where firstUpdatedDay_record = '13'
          and  parameter = 'pm25'
          ORDER BY average DESC 
          limit 5  """)

result.show()

# Query 3

result = spark.sql("""SELECT city, AVG(average) AS mean,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY average) AS median
                      FROM split_date_df 
                      WHERE firstUpdatedHour_record = '15'
                        AND parameter = 'pm25'
                          GROUP BY city
                            ORDER BY mean DESC
                            LIMIT 10;""")


result.show()

# Query 4
result = spark.sql("""

WITH Percentile_ct AS (
  SELECT
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY average) AS 90th_percentile
  FROM
    split_date_df
)

SELECT distinct city
FROM
  split_date_df
WHERE
  average < (SELECT 90th_percentile FROM Percentile_ct);
""")
result.show()
