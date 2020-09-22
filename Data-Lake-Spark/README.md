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

## Local Spark Development Environment
- Download latest version (3.0.01 Sep 02,2020), package type (Pre-built for Apache Hadoop 3.2 and later), download spark: `spark-3.0.1-bin-hadoop3.2.tgz`
- Then unzip in to `/opt`, set in `~/.zshrc` for `export SPARK_HOME="/opt/spark-3.0.1-bin-hadoop3.2"`

```bash
# fxrc @ pop in ~ [13:02:30] C:130
$ sudo tar -xf ~/Downloads/spark-3.0.1-bin-hadoop3.2.tgz -C /opt/
[sudo] password for fxrc:
# fxrc @ pop in ~ [13:02:37]
$ sudo chmod -R 755 /opt/spark-3.0.1-bin-hadoop3.2
# fxrc @ pop in /opt [13:05:09]
$ . ~/.zshrc
# fxrc @ pop in /opt [13:05:15]
$ echo ${SPARK_HOME}
/opt/spark-3.0.1-bin-hadoop3.2
```

- Ready to launch Spark in standalone mode locally: `${SPARK_HOME}/sbin/start-all.sh`

```bash
# fxrc @ pop in ~/Learn/UdacityNanodegree/Udacity-Data-Engineering/Data-Lake-Spark on git:master x [13:23:45]
$ ./run.sh
starting org.apache.spark.deploy.master.Master, logging to /opt/spark-3.0.0-bin-hadoop3.2/logs/spark-fxrc-org.apache.spark.deploy.master.Master-1-pop.out
localhost: starting org.apache.spark.deploy.worker.Worker, logging to /opt/spark-3.0.0-bin-hadoop3.2/logs/spark-fxrc-org.apache.spark.deploy.worker.Worker-1-pop.out
```

- Verify it's good, according to: https://jimolonely.github.io/2019/09/15/spark/008-spark-standalone-conf-run/
    - or goto Spark UI: http://localhost:8080/
    - or use `jps` in terminal to check java process.

    ```bash
    # fxrc @ pop in ~/Learn/UdacityNanodegree/Udacity-Data-Engineering/Data-Lake-Spark on git:master x [13:24:29]
    $ jps
    800335 Jps
    7349
    798740 Worker
    798469 Master
    ```