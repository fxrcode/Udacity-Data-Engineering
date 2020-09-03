# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songsplays;"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = """
CREATE TABLE songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INTEGER NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
)
"""

user_table_create = """
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    geneder VARCHAR,
    level VARCHAR
)
"""

song_table_create = """
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INTEGER,
    duration FLOAT
)
"""

artist_table_create = """
CREATE TABLE artist (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
)
"""

time_table_create = """
CREATE TABLE tim (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
)
"""

# INSERT RECORDS

songplay_table_insert = """
"""

user_table_insert = """
"""

song_table_insert = """
"""

artist_table_insert = """
"""


time_table_insert = """
"""

# FIND SONGS

song_select = """
"""

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

