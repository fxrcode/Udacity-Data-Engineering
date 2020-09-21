import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """load input JSON data: LOG_DATA, SONG_DATA from S3
    into Redshift tables: staging_events and staging songs given speicific schema

    Args:
        cur (psycopg2.cursor): Allows Python code to execute PostgreSQL command in a database session.
        conn (psycopg2.connection): Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """parallelize data ingestion from staging to STAR schema for descision making in higher performance.
        fact table: songplays
        dimension tables: time, users, songs, artists.

    Args:
        cur (psycopg2.cursor): Allows Python code to execute PostgreSQL command in a database session.
        conn (psycopg2.connection): Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """configure and init redshift postgres DB connection with provided info from IaC ipynb to create redshift cluster.
        2 phases ETL:
        * load_staging: JSON in S3 to Staging DB in redshift
        * insert_tables: build STAR schema for business analytics from staging DB in Redshift cluster.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()