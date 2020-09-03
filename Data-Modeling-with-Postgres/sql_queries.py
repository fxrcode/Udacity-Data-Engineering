# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songsplays;"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = """
CREATE TABLE songplays (
    songplay_id SERIAL,
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER NOT NULL,
    location VARCHAR,
    user_agent VARCHAR,
    PRIMARY KEY (songplay_id),
    CONSTRAINT fk_time
        FOREIGN KEY (start_time)
            REFERENCES time(start_time),

    CONSTRAINT fk_user
        FOREIGN KEY (user_id)
            REFERENCES users(user_id),

    CONSTRAINT fk_song
        FOREIGN KEY (song_id)
            REFERENCES songs(song_id),

    CONSTRAINT fk_artist
        FOREIGN KEY (artist_id)
            REFERENCES artists(artist_id)
)
"""

user_table_create = """
CREATE TABLE users (
    user_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR,

    PRIMARY KEY (user_id)
)
"""

song_table_create = """
CREATE TABLE songs (
    song_id VARCHAR,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INTEGER,
    duration FLOAT,
    PRIMARY KEY (song_id)
)
"""

artist_table_create = """
CREATE TABLE artists (
    artist_id VARCHAR,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (artist_id)
)
"""

time_table_create = """
CREATE TABLE time (
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time)
)
"""

# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

user_table_insert = """
INSERT INTO users(user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO UPDATE SET level = EXCLUDED.level
"""

song_table_insert = """
INSERT INTO songs(song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO NOTHING
"""

artist_table_insert = """
INSERT INTO artists(artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO NOTHING
"""


time_table_insert = """
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time)
DO NOTHING
"""

# FIND SONGS

song_select = """
SELECT s.song_id, a.artist_id FROM songs s
LEFT JOIN artists a ON s.artist_id = a.artist_id
WHERE
    s.title = %s
    AND
    a.name = %s
    AND
    s.duration = %s
"""

# QUERY LISTS

create_table_queries = [
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

