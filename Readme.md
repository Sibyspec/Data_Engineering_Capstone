Project Overview
================

Airbnb has grown their Listings and Hosts database and want to move their processes and data onto the cloud. 
Their data resides in S3 storage as CSV files.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, 
processes them using Airflow, and loads the DWH database with Staging, Fact and Dimension tables. 

This will allow their analytics team to continue finding insights into their Listings, Hosts and Reviews they get on their websites.

Below are few of many details that can be answered by this 

   1. The ratings of the listed objects?
   2. The host location compared to the object location?
   3. Details of the objects listed ?
   4. Review posted by customers ? etc

Data assessment
===============

Data Source : http://insideairbnb.com/get-the-data.html

Airbnb provides free datasets for the usage of public.

1. The data is present in the .csv files which are downloaded from the airbnb website uploaded to the S3 bucket.

2. There are two .csv files. One file (listings.csv) has the listings data and the second file (reviews.csv) has the reviews data.
 
3. The csv files has some columns which are empty, which should be handled in the code.

4. The date columns in the csv files are not in date format, so a custom formula was used to populate the dates.
   
   Date calculation formula : to_date('01-01-2020','DD-MM-YYYY')-(date column/100)

5. The review file is bigger in size due to huge number of reviews written. Reviews can be of more than 10000 bytes so varchar(65535) was  used for the review field and the field for the description of the host.

6. The listings file is relatively smaller as it does not have any description fields.

Database Design
===============

Database used : Amazon Redshift

Redshift Cluster Design
-----------------------

Cluster name    : sibyredshift

Type            : dc2.large

Nodes           : 1

Datamodel Design
----------------

DWH schema used   : Star

Staging table     : STAGING_LISTINGS, STAGING_REVIEWS

Fact              : LISTINGS

Dimenstions       : HOSTS, LISTING_ADDRESS, LISTING_REVIEW, LISTING_PROPERTY_TYPE, LISTING_ROOM_TYPE

Fact and Dimension Table Description
------------------------------------

LISTINGS
---------

This table records all the objects that are listed. It contains details like lisitngId, ListingType, HostId  etc.

![listings](https://user-images.githubusercontent.com/63420444/78941433-f8ccda00-7ab7-11ea-9608-cabe70405d8c.JPG)



HOSTS
-----
This table has the list of all the hosts available in the Airbnb. It has the details like hostid, host name, host address etc.

![hosts](https://user-images.githubusercontent.com/63420444/78940584-5f50f880-7ab6-11ea-87da-cfa578fe1e5f.JPG)


LISTING_ADDRESS
--------------
This table has the address details of all the objects listed in Airbnb.

![address](https://user-images.githubusercontent.com/63420444/78940961-06359480-7ab7-11ea-92b4-a3a11436f529.JPG)


LISTING_REVIEW
--------------
This table has the details of all the reviews made by the customers on the listing.

![review](https://user-images.githubusercontent.com/63420444/78941063-37ae6000-7ab7-11ea-9591-ca9b9785f94e.JPG)



LISTING_PROPERTY_TYPE
---------------------
This table holds the details of the type of properties available in Airbnb.

![property](https://user-images.githubusercontent.com/63420444/78941140-60cef080-7ab7-11ea-81a9-cd5fc8318709.JPG)



LISTING_ROOM_TYPE
-----------------
This table holds the details of the type of rooms available in Airbnb.


![room](https://user-images.githubusercontent.com/63420444/78941215-822fdc80-7ab7-11ea-88b1-a419a8dab63e.JPG)



DAG DESIGN
==========

Description       : AIRBNB_DAG

schedule_interval : Daily

Start Date        : 08-Apr-2020

Catchup           : False

depends_on_past   : False

Retries           : 3 times (Every 5 mins)


![DAG](https://user-images.githubusercontent.com/63420444/78940160-b0142180-7ab5-11ea-94b7-a13013988f41.JPG)

Create Tables            : This step creates all the tables needed for the ETL process.

Stage Listing            : This step loads all the listing data into the staging_listings table.

Stage Reviews            : This step loads all the listing data into the staging_reviews table.

Load listings fact table : This step loads the fact table LISTINGS.

Load property dim table  : This step loads the dim table LISTING_PROPERTY_TYPE.

Load hosts dim table     : This step loads the dim table HOSTS.

Load room dim table      : This step loads the dim table LISTING_ROOM_TYPE

Load address dim table   : This step loads the dim table LISTING_ADDRESS

Load reviews dim table   : This step loads the dim table LSITING_REVIEWS

Run data quality checks  : This step runs the data quality checks on fact and sim tables.


Python Files
============

arbnb_dag.py        : This file contains the DAG and it scheduling details. Also this acts as the central controller of ETL process.
                      It can be found inside the airflow/dags directory.

stage_reshift.py    : This file contains the code for custom operator 'StageToRedshiftOperator'.
                      It can be found inside the plugins/operators directory.

load_fact.py        : This file contains the code for custom operator 'LoadFactOperator'.
                      It can be found inside the plugins/operators directory.

load_dimension.py   : This file contains the code for custom operator 'LoadDimensionOperator'.
                      It can be found inside the plugins/operators directory.

data_quality.py     : This file contains the code for custom operator 'DataQualityOperator'.
                      It can be found inside the plugins/operators directory.
                      
sql_queries.py      : This is helper file contains the SQLs for inserting data into the fact and dimension tables.
                      It can be found inside the plugins/helpers directory.
                      
                      
SQL Files
=========

create_airbnb_tables.sql   : This SQL file holds the query for table creations. All the staging, fact and dimension tables.

Choice Of Technologies
======================

Why Redshift ?
------------
Amazon Redshift was used as a cloud Data Warehouse because it is highly scalable. If we needed to process 100x the amount of data we currently have, Redshift would be able to handle this by allowing us to scale both the size and number of nodes in the cluster as we wish.

Why S3?
------
Amazon S3 was chosen to store the CSV files since Redshift has built-in support for extracting data from S3 and it is relatively easy to set up access controls inside AWS.

Why Airflow?
------------
Airflow was chosen for this project since it allows us to build complex data pipelines in a straightforward, modular manner. One can separate the different stages of the pipeline into distinct tasks and define dependencies between them, so that some tasks can run in parallel while other tasks wait for upstream tasks before executing. It also allows us to make our initial solution scalable so that we could process data in different batches based on the timestamp of the data. The Airflow UI enables users to inspect and analyse the different steps of the pipeline and easily check if something has gone wrong.

Approaches to Different Problems
================================

1.If the data was increased by 100x ?

  Depending on the data, we can try different partitioning methods and also increase the number of nodes in redshift cluster.
  
2.If the pipelines were run on a daily basis by 7am ?

  The schedule needs to be changed according to the timings and SLA.
  
3.If the database needed to be accessed by 100+ people.?

  Change the concurrent usage parameter based on the load.

How to Run ?
============

Runs automatically everyday in Airflow. Also can be run manually in airflow b clickcing the play icon
