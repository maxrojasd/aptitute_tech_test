# TLC_Trip_Records_Service APP
Script to get the top N records from a specific TLC line and fix the total amount of the trips.

## Pre-requirements
In order to run the script, make sure to have the following:
- Spark
- boto3
- s3fs
- Request
- pytest

## How-to-run
Just run the main.py script and wil run script

# Unit Test
Not Implemented this time, just included the framework

## The approach

This code is intended to fetch data from openAQ, upload it to S3 and then create tables in Redshift to be able to provide a scalable solution for the BA team. 

This solution works with serverless resources and in case is needed it is also working with cloud-based processing architecture considering the possibility to either run in public networks or VPCs. For this, the main storage service to be used is S3, where a cluster must be created to host the files that will be processed by the script, once the files are there, they will be transderred to Redshift to a specific table where the data can be reviewed by DAs using lambda functions. The idea was to create a simple and cheap approach that can work with constant low intensity workloads. More details below.


## The architecture

As the idea is to have a system with just a small sample dataset available for the BAs to analyse, the infrastructure is intended to work with small datasets but with a scalable framework in case the volume of data increase considerably. The architecture consist in a first version where it can works mostly with serverless tools but I also added the ideal architecture for the next stage when the volume of data and transactions increase considerably. 

The ETL works by reading data or files either from openAQ API or specific files, transform the data, create the table in redshift with the schema of the transformed table, then upload the table as a parquet file in a S3  bucket where it triggers a lambda function that updates the records from the parquet files in the previously created Redshift's Table in the data mart.


![](https://github.com/maxrojasd/aptitute_tech_test/blob/openaq_test/img/diagram1.png)


- Python Scripts: There is a API framework to run custom operations using boto3 to interact directly between pyspark and AWS services, it also includes the possibility of an orchestrator for higly customizable python scripts, in case the complexity of the architecture increase given the nature of the demand for the Cloud-based solution. At a first stage this will run a crawler to get the data and upload it to S3 and create the corresponding redshift table on the cluster.

- S3: The architecture use S3 as the main storage service because of its scalability and serverless triggers, here all the data will be uploaded after running the previous python scripts or any other ingestion API that may be suitable in the future. S3 will send an event to a lambda function to update the previously created table in Redshift's cluster.

Lambda Function: There is only one configured to work each time a new file is added to S3, it will connect to the Redshifts Cluster's VPC and update the records in the corresponding table. ( Script can be found in folder lambda in root)

VPC: It is configured to accept connections from S3 and Lambda functions via IAM.

Gateaway: There is a gateaway created to accept connections between services.


Costs: In order to assess the cheapest and fastest solution given the requirements, The minimum capacity for most of the services will be enough. Just for Referencial purposes:

The initial CSV File was 90mb, but, saved as parquet file is 1.2mb
When using the API to fetch openAQ data, this can be saved directly as parquet files, therefore, if we run the solution once per day, every month there will be around 36mb of records
if everyday the system upload the records 10 times, this number will increase to 360mb.
Considering this number of requests for all the other services that are activated asynchronously we can get to the quote below.

In case the Redshift Cluster is serverless:

![](https://github.com/maxrojasd/aptitute_tech_test/blob/openaq_test/img/cost1.png)

In case the Redshift Cluster is not serverless:

![](https://github.com/maxrojasd/aptitute_tech_test/blob/openaq_test/img/cost.png)

Here it is the biggest difference for the total costs of the solution as it is right now. 


AS the solution requires more and more resources the solution's next step that I proposed is the the one in the diagram below.


![](https://github.com/maxrojasd/aptitute_tech_test/blob/openaq_test/img/diagram2.png)

What are the upgrades? It would include AWS Glue in order to democratise the ETL Process between S3 and Redshift avoiding potential errors as it will include a schema validation & transformation process. It will also allow to the system the possibility of distribute the data between different data marts and data sources given the needs. 

It would kick-off each time when receives a file in S3, where it will then run a crawler in Glue to get the data uploaded into a glue catalog and the S3 bucket, when this is done, it creates an event trigger that automatically runs a second lambda function to run a glue job that connects with the VPC and updates the table in Redshift.

This option will demand more running time than the first option, therefore, if the running time of the architecture implemented is not even close to the GLUE ETL one, it will be more costly to run than the first option.

The advantage of this one, is the scalability as it will work more efficienly with several GBs of workload rather than the previous option that will be slower when the volume of data gets to this levels.



Costs:

This solution Includes:
- 10gb of monthly storage
- 100000 Lambda requests
- AWS Glue for 10 etl jobs of 10 mins each day = 2 crawlers
- 1 Redshift dedicated cluster (dc2.large) total use 1 hour/day 
- 100000 Eventbridge events

![](https://github.com/maxrojasd/aptitute_tech_test/blob/openaq_test/img/cost3.png)

## Configurations

Many of the configurations are in the folder img/screenshots_config. It would be ideal to include IAC wtih Terraform or similar in a future stage.

## Data Quality

There are some Data transformations needed in order to improve data quality, this includes:

- Manage column names
- Handling null values
- Transform columns
- Check Format
- Duplication check

There was also some columns that were removed as they were either duplicated or present irrelevant data: those are the following:

- '_c8': Duplicated column (timestamp)
- '_c9': Irrelevant, always government
- '_c10': Irrelevant, always false
- '_c13': Number of hours for period of measurement
- '_c14': unit for _c13

## Data Schema

The Schema created in Redshift can be found in the screenshot below 


![](https://github.com/maxrojasd/aptitute_tech_test/blob/openaq_test/img/schema.png)


## Queries

The Queries are written below for SQL queries. There is also a script that includes sql queries to run with Pyspark


### query 1

```
WITH percentile_table AS (SELECT
    month, location,city,pollutant,date, avg(value) as average,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY AVG(value)) 
                          OVER (PARTITION BY EXTRACT(MONTH FROM TO_DATE(date, 'YYYY-MM-DD'))) <= AVG(value) AS "90TH_PERCENTILE"
FROM
    YOUR_TABLE
GROUP BY month, location, city, pollutant,date)
    
SELECT DISTINCT city FROM percentile_table 
WHERE
 percentile_table."90th_percentile" = True
 AND
percentile_table."pollutant" in ('no2','co')
AND
percentile_table."month" = '08'
```



### query 2

```
WITH MONTHLY_AVERAGE_TABLE AS (
    SELECT
    city, value,pollutant,date,
    AVG(value) OVER (PARTITION BY EXTRACT(DAY FROM TO_DATE(date, 'YYYY-MM-DD'))) AS monthly_average
FROM
    YOUR_TABLE
WHERE
    TO_DATE(date, 'YYYY-MM-DD') = '2017-08-16'
    AND
    YOUR_TABLE."pollutant" = 'pm25'
ORDER BY monthly_average, value DESC
)

SELECT distinct city
FROM
     MONTHLY_AVERAGE_TABLE
ORDER BY monthly_average, value DESC
limit 5
```

### query 3

```
WITH hourly_average_table AS (
    SELECT
    *,
    AVG(value) OVER (PARTITION BY hour) AS hourly_average
FROM
    YOUR_TABLE
WHERE
    hour = '11'
    AND
    YOUR_TABLE."pollutant" in ('pm25')
),

mode_table AS (
    SELECT
        city, hour,
        value,
        COUNT(*) AS mode
    FROM
        YOUR_TABLE
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
    YOUR_TABLEoriginal_table
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
```

### query 4

```
WITH hourly_average_table AS (
    SELECT
    *
FROM
    YOUR_TABLE
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
```
