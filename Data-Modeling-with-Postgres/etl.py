import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Function to process song json so as to upsert songs table and artists table

    Args:
        cur (pyscopg2.cursor): Allows Python code to execute PostgreSQL command in a database session.
        filepath (string): path of song.json
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Function to process log.json to upsert time table, users table, and songplays table

    Args:
        cur (pyscopg2.cursor): Allows Python code to execute PostgreSQL command in a database session.
        filepath (string): path of log.json
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    # https://stackoverflow.com/questions/17071871/how-to-select-rows-from-a-dataframe-based-on-column-values
    df = df.loc[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")

    # insert time data records
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html
    # https://pandas.pydata.org/docs/user_guide/merging.html
    time_data = pd.concat(
        [
            t,
            t.dt.hour,
            t.dt.day,
            t.dt.isocalendar().week,
            t.dt.month,
            t.dt.year,
            t.dt.weekday,
        ],
        axis=1,
    ).values
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        # FIXME: https://knowledge.udacity.com/questions/48698
        # results = cur.execute(song_select, (row.song, row.artist, row.length))
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        if results:
            """
            loop 30 files and only 1 songid, artistid is not null
            28/30 files processed.
            ('SOZCTXZ12AB0182364', 'AR5KOSW1187FB35FF4')
            """
            print(f"songid: {songid}, artistid: {artistid}")

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit="ms"),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except Exception as e:
            """
            Error: Issue songplay_table_insert (1541191397796, 3, 'free', None, None, 112, 'Saginaw, MI', 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20100101 Firefox/31.0')
not all arguments converted during string formatting
            """
            print("Error: Issue songplay_table_insert", songplay_data)
            print(e)


def process_data(cur, conn, filepath, func):
    """meta function to prepare the right song and log json filepaths, and feed them to process_log_file, process_song_file for ETL process

    Args:
        cur (pyscopg2.cursor): Allows Python code to execute PostgreSQL command in a database session.
        conn (pyscopg2.connection): Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
        filepath (string): directory path of song_data and log_data.
        func (function): accept process_[song/log]_file functions.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        # print(f"processing path: {datafile}")
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
