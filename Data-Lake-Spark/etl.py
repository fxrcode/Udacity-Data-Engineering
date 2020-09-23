import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
import logging

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s  [%(name)s] %(message)s')
LOG = logging.getLogger('etl')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """extract song and artists table from song_data directly from s3. song_data folder is similar to Data Warehouse staging_song.

    Args:
        spark (SparkSession): The entry point to programming Spark with the Dataset and DataFrame API.
        input_data (string): input data root path (local test) or S3 bucket (udacity)
        output_data ([type]): output data root path (local test) or S3 bucket (my own)
    """
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"
    LOG.info(f"Here you go, song_data: {song_data}")

    # read song data file
    df_s = spark.read.json(song_data)

    ## quick inspect
    df_s.printSchema()
    df_s.show(5)

    ## Creates or replaces a local temporary view with this DataFrame, so as to use pyspark.sql
    df_s.createOrReplaceTempView("song_view")

    # extract columns to create songs table
    songs_table = spark.sql("""
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM song_view
    """)
    songs_table.printSchema()
    songs_table.show(5)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path=output_data+'song_table', mode="overwrite")

    ## createTempView everytime before spark.sql, because the df might have been modified with new columns
    df_s.createOrReplaceTempView("song_view")

    # extract columns to create artists table
    artists_table = spark.sql("""
SELECT DISTINCT artist_id, 
                artist_name,
                artist_location, 
                artist_latitude,
                artist_longitude
FROM song_view
""")
    artists_table.printSchema()
    artists_table.show(5)

    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists.parquest', mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    # log_data = f"{input_data}log_data/*/*/*.json"
    log_data = f"{input_data}log_data/*.json"
    LOG.info(f"Here you go, log_data: {log_data}")

    # read log data file
    df_l = spark.read.json(log_data)

    # filter by actions for song plays
    df_l = df_l.filter(df_l.page == 'NextSong')

    # createTempView from df_l
    df_l.createOrReplaceTempView("log_view")

    # extract columns for users table
    users_table = spark.sql("""
    SELECT DISTINCT userId as user_id,
                    firstName as first_name,
                    lastName as last_name,
                    gender,
                    level
    FROM log_view
    """)
    users_table.printSchema()
    users_table.show(5)

    # write users table to parquet files
    users_table.write.parquet(output_data+"users.parquet", mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df_l = df_l.withColumn("timestamp", get_timestamp(df_l.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df_l = df_l.withColumn("datetime", get_datetime(df_l.ts))

    ## quick inspect
    df_l.printSchema()
    df_l.show(5)

    ## createTempView from log data for spark.sql
    df_l.createOrReplaceTempView("log_view")

    # extract columns to create time table
    time_table = spark.sql("""
    SELECT  DISTINCT timestamp AS start_time,
                        hour(timestamp) AS hour,
                        day(timestamp)  AS day,
                        weekofyear(timestamp) AS week,
                        month(timestamp) AS month,
                        year(timestamp) AS year,
                        dayofweek(timestamp) AS weekday
    FROM log_view
    """)
    time_table.printSchema()
    time_table.show(5)

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data+"time.parquet", mode = "overwrite")

    # read in song data to use for songplays table
    song_data = f"{input_data}song_data/*/*/*/*.json"
    LOG.info(f"Here you go, song_data: {song_data}")
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    df_l.createOrReplaceTempView("logs")
    song_df.createOrReplaceTempView("songs")
    time_table.createOrReplaceTempView("time")

    songplays_table = spark.sql("""
    SELECT
        monotonically_increasing_id() as songplay_id,
        l.datetime,
        l.userId as user_id,
        l.level,
        s.song_id,
        s.artist_id,
        l.sessionId as session_id,
        l.location,
        l.userAgent,
        t.year,
        t.month
    FROM logs l
    JOIN songs s ON l.song = s.title AND l.artist = s.artist_name AND l.length = s.duration
    JOIN time t ON l.timestamp = t.start_time
    """)

    # df_sp = df_l.join(df_s, (df_l.artist == df_s.artist_name) & (df_l.song == df_s.title) & (df_l.length == df_s.duration))
    # df_sp = df_sp.join(time_table, (df_sp.datetime == time_table.start_time))

    # songplays_table = songplays_table.withColumn('songplay_id', )
    songplays_table.printSchema()
    songplays_table.show(5)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data+"songplays.parquet", mode = "overwrite")


def main():
    spark = create_spark_session()
    LOG.info(f"Spark launched:\n {spark} \n")
    # input_data = "s3a://udacity-dend/"
    # output_data = ""
    CWD = os.getcwd()
    input_data = f"{CWD}/data/"
    output_data = f"{CWD}/output_data/"

    t_song_data = datetime.now()
    process_song_data(spark, input_data, output_data)
    LOG.info(f"Finished process_song_data in {datetime.now() - t_song_data}")
    t_log_data = datetime.now()
    process_log_data(spark, input_data, output_data)
    LOG.info(f"Finished process_log_data in {datetime.now() - t_log_data}")

    LOG.info(f"Total process took {datetime.now() - t_song_data}")


if __name__ == "__main__":
    main()
