# Data Pipelines with Airflow

## Intro
* A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

* They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

* The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Overview
* This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

* We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

* You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

## Star schema
![](dp.png)

## Steps
### AWS Redshift creation
* using `setup-cluster.ipynb` to launch a new 4 node d2.large Redshift cluster.
    * copy endpoint and role ARN.
    * run Project 2's `create_table.py` to DROP tables, then CREATE 5 tables in redshift.

## Airflow config
* On the create connection page, enter the following values. Once you've entered these values, select Save and Add Another.
```
Conn Id: Enter aws_credentials.
Conn Type: Enter Amazon Web Services.
Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.
```
* On the next create connection page, enter the following values:
```
Conn Id: Enter redshift.
Conn Type: Enter Postgres.
Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
Schema: Enter dev. This is the Redshift database you want to connect to.
Login: Enter awsuser.
Password: Enter the password you created when launching your Redshift cluster.
Port: Enter 5439.
```

## Success!
![](airflow0.png)
![](airflow1.png)

## Issues
* in load_fact, it failed due to. The reason is the songplays's songplay_id is IDENTITY, just make it `playid varchar(32)`. Then we good.
```
[2020-09-25 10:13:31,923] {models.py:1593} INFO - Executing <Task(LoadFactOperator): Load_songplays_fact_table> on 2020-09-25T09:00:00+00:00
[2020-09-25 10:13:31,924] {base_task_runner.py:118} INFO - Running: ['bash', '-c', 'airflow run etl_dag_1 Load_songplays_fact_table 2020-09-25T09:00:00+00:00 --job_id 68 --raw -sd DAGS_FOLDER/udac_example_dag.py --cfg_path /tmp/tmp0me3r9ss']
[2020-09-25 10:13:32,527] {base_task_runner.py:101} INFO - Job 68: Subtask Load_songplays_fact_table [2020-09-25 10:13:32,527] {settings.py:174} INFO - settings.configure_orm(): Using pool settings. pool_size=5, pool_recycle=1800, pid=22780
[2020-09-25 10:13:33,335] {base_task_runner.py:101} INFO - Job 68: Subtask Load_songplays_fact_table [2020-09-25 10:13:33,334] {__init__.py:51} INFO - Using executor LocalExecutor
[2020-09-25 10:13:33,611] {base_task_runner.py:101} INFO - Job 68: Subtask Load_songplays_fact_table [2020-09-25 10:13:33,610] {models.py:273} INFO - Filling up the DagBag from /home/workspace/airflow/dags/udac_example_dag.py
[2020-09-25 10:13:33,651] {base_task_runner.py:101} INFO - Job 68: Subtask Load_songplays_fact_table [2020-09-25 10:13:33,651] {cli.py:520} INFO - Running <TaskInstance: etl_dag_1.Load_songplays_fact_table 2020-09-25T09:00:00+00:00 [running]> on host e5ca5353577b
[2020-09-25 10:13:33,726] {load_fact.py:29} INFO - LoadFactOperator start
[2020-09-25 10:13:33,726] {load_fact.py:32} INFO - Truncate fact table: songplays
[2020-09-25 10:13:33,934] {logging_mixin.py:95} INFO - [2020-09-25 10:13:33,933] {base_hook.py:83} INFO - Using connection to: id: redshift. Host: dpcluster.cei2sezqr7kl.us-west-2.redshift.amazonaws.com, Port: 5439, Schema: dp, Login: dwhuser, Password: XXXXXXXX, extra: {}
[2020-09-25 10:13:34,401] {logging_mixin.py:95} INFO - [2020-09-25 10:13:34,401] {dbapi_hook.py:166} INFO - TRUNCATE TABLE songplays
[2020-09-25 10:13:34,679] {load_fact.py:34} INFO - Arm songplays fact table
[2020-09-25 10:13:34,697] {logging_mixin.py:95} INFO - [2020-09-25 10:13:34,697] {base_hook.py:83} INFO - Using connection to: id: redshift. Host: dpcluster.cei2sezqr7kl.us-west-2.redshift.amazonaws.com, Port: 5439, Schema: dp, Login: dwhuser, Password: XXXXXXXX, extra: {}
[2020-09-25 10:13:35,153] {logging_mixin.py:95} INFO - [2020-09-25 10:13:35,153] {dbapi_hook.py:166} INFO - 
                INSERT INTO songplays
                
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    ;
[2020-09-25 10:13:35,283] {models.py:1788} ERROR - cannot set an identity column to a value
Traceback (most recent call last):
  File "/opt/conda/lib/python3.6/site-packages/airflow/models.py", line 1657, in _run_raw_task
    result = task_copy.execute(context=context)
  File "/home/workspace/airflow/plugins/operators/load_fact.py", line 39, in execute
    redshift.run(formatted_sql)
  File "/opt/conda/lib/python3.6/site-packages/airflow/hooks/dbapi_hook.py", line 170, in run
    cur.execute(s)
psycopg2.NotSupportedError: cannot set an identity column to a value

[2020-09-25 10:13:35,286] {models.py:1811} INFO - Marking task as UP_FOR_RETRY
```