# Project: Data Lake
## Introduction
* A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

* As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

* You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
* In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## My track
### Develop flow
* AWS S3 will be the data source and destination. You will read the songs and events files from AWS S3, process them into 5 tables and write the tables data back in parquet format to AWS S3 while satisfying the following requirements
    - Separate directory for each table
    - The right columns per table with the right partitioning
    - The duplicates should be handled in the proper manner.
* The data inside the workspace can be used for testing; you create the code on it instead fetching and loading from/to AWS S3. once you complete your development phase on local data, change the paths to read from and write to AWS S3.

### Project Workspace and EMR clusters
* For develop, you can do the project inside the Udacity workspace without an EMR cluster.
    * You can use the locally available data inside worksapce or use the S3 bucket of Udacity. Using the local data will be faster.
    * If you want to save the data to S3 you need to create an S3 bucket, otherwise you can write the output files locally.
    * I would recommend starting with the local and once you know that the ETL process is working as expected, then move to the S3 data.

* Exactly, you are using Spark locally. And of course, you need to create an S3 bucket on you AWS account to hold the output.

### [EMR cluster](https://knowledge.udacity.com/questions/307548)
* You are supposed to develop your code using local data in the workspace, then upload it onto the EMR master after you create the cluster on AWS.
* Once your code works on the local dir data with a single Spark local node, you can add your AWS credentials and change the input/output path to S3 (you need to create an S3 bucket for outputs), then run your code on AWS EMR.

### [Run etl.py](https://knowledge.udacity.com/questions/113108)
* choose `EMR-6.0.0` for "spark-submit" to work properly with Python 3.
* After EMR spinned, ssh to it, then do `spark-submit etl.py`.