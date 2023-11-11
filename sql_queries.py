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
result = spark.sql(f"""
WITH percentile_table AS (SELECT
    month, location,city,pollutant,date, avg(value) as average,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY AVG(value)) 
                          OVER (PARTITION BY EXTRACT(MONTH FROM TO_DATE(date, 'YYYY-MM-DD'))) <= AVG(value) AS "90TH_PERCENTILE"
FROM
    "dev"."public"."openaq_table25"
GROUP BY month, location, city, pollutant,date)
    
SELECT DISTINCT city FROM percentile_table 
WHERE
 percentile_table."90th_percentile" = True
 AND
percentile_table."pollutant" in ('no2','co')
AND
percentile_table."month" = '08'
""")
result.show()


# query 2
result = spark.sql(
    """
WITH MONTHLY_AVERAGE_TABLE AS (
    SELECT
    city, value,pollutant,date,
    AVG(value) OVER (PARTITION BY EXTRACT(DAY FROM TO_DATE(date, 'YYYY-MM-DD'))) AS monthly_average
FROM
    "dev"."public"."openaq_table25"
WHERE
    TO_DATE(date, 'YYYY-MM-DD') = '2017-08-16'
    AND
    "dev"."public"."openaq_table25"."pollutant" = 'pm25'
ORDER BY monthly_average, value DESC
)

SELECT distinct city
FROM
     MONTHLY_AVERAGE_TABLE
ORDER BY monthly_average, value DESC
limit 5

          """)

result.show()

# Query 3

result = spark.sql("""
WITH hourly_average_table AS (
    SELECT
    *,
    AVG(value) OVER (PARTITION BY hour) AS hourly_average
FROM
    dev.public.openaq_table25
WHERE
    hour = '11'
    AND
    "dev"."public"."openaq_table25"."pollutant" in ('pm25')
),

mode_table AS (
    SELECT
        city, hour,
        value,
        COUNT(*) AS mode
    FROM
        dev.public.openaq_table25
    GROUP BY
        value, city, hour
    ORDER BY
        COUNT(*) DESC
    LIMIT 1
    )

SELECT DISTINCT original_table.city, original_table.pollutant,
    AVG(original_table.value) OVER (PARTITION BY original_table.hour) AS mean,
    PERCENTILE_CONT(0.5 ) WITHIN GROUP (ORDER BY original_table.value) OVER (PARTITION BY original_table.hour) AS median,
    mode_table.mode

FROM
    dev.public.openaq_table25 original_table
JOIN 

    mode_table

ON original_table.hour = mode_table.hour

JOIN
    (
        SELECT DISTINCT city
        FROM 
        hourly_average_table
        ORDER BY value DESC
        LIMIT 10
    ) filtered_table
ON
    original_table.city = filtered_table.city

where 
    "pollutant" in ('co','no2')
""")


result.show()

# Query 4
result = spark.sql("""
WITH hourly_average_table AS (
    SELECT
    *
FROM
    dev.public.openaq_table25 
WHERE
    hour = '11'

),
 minmax_table  AS (
    SELECT *,
    MAX(value) OVER (PARTITION BY pollutant) AS max_val,
    MIN(value) OVER (PARTITION BY pollutant) AS min_val,
    AVG(value) OVER (PARTITION BY pollutant) AS mean_val
FROM
    hourly_average_table
)


SELECT
    country,city, value, min_val, max_val, mean_val,
    CASE
        WHEN value <= (max_val - min_val) / 3 THEN 'Low'
        WHEN value > ((max_val - min_val) / 3) * 2 THEN 'High'
        ELSE 'Moderate'
    END AS label
FROM
    minmax_table;
""")
result.show()
