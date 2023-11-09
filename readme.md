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

This solution works with serverless resources and in case is needed it is also working with cloud-based processing architecture considering the possibility to either run in public networks or VPCs. For this, the main storage service to be used is S3, where a cluster must be created to host the files that will be processed by the script, once the files are there, they will be transderred to Redshift to a specific table where the data can be reviewed by DAs 


## The architecture

The basic architecture for this project is intended to reduce costs, but with the consideration it might need to be scaled up when the demand is higher. As a starting point, the architecture only includes S3 and Redshift with the idea to automate some processes with AWS Lambda in a next stage.
![](https://github.com/maxrojasd/aptitute_tech_test/blob/openaq_test/architecture.jpg?raw=true)

## Data Quality

There are some Data transformations needed in order to improve data quality, this includes:

- Handling null values
- Transform columns
- Check Format
- Duplication check

## Data Schema

Below is the Created Table schema.

+------------------------+------------------------+
|      Column Name       |      Data Type         |
+------------------------+------------------------+
| manufacturername       | character varying      |
| modelname              | character varying      |
| firstupdated           | character varying      |
| lastupdated            | character varying      |
| displayname            | character varying      |
| parameter              | character varying      |
| unit                   | character varying      |
| bounds                 | character varying      |
| firstupdated_record    | character varying      |
| lastupdated_record     | character varying      |
| sensortype             | character varying      |
| isanalysis             | character varying      |
| sources                | character varying      |
| country                | character varying      |
| entity                 | character varying      |
| name                   | character varying      |
| city                   | character varying      |
| longitude              | double precision       |
| latitude               | double precision       |
| lastvalue              | double precision       |
| average                | double precision       |
| lastupdatedsecond_record | integer              |
| lastupdatedminute_record | integer              |
| lastupdatedhour_record  | integer               |
| lastupdatedday_record   | integer               |
| lastupdatedmonth_record | integer               |
| lastupdatedyea_record   | integer               |
| firstupdatedsecond_record | integer              |
| firstupdatedminute_record | integer              |
| firstupdatedhour_record  | integer               |
| firstupdatedday_record   | integer               |
| firstupdatedmonth_record | integer               |
| firstupdatedyear_record  | integer               |
| parameterid            | bigint                 |
| count                  | bigint                 |
| id                     | bigint                 |
| measurements           | bigint                 |
| id_record              | bigint                 |
| ismobile               | boolean                |
+------------------------+------------------------+


## Queries

The Queries that were created as example can be found in the script sql_queries.py, these ones need to be refined.
