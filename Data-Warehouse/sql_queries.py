import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
SONGS_JSONPATH = config.get("S3", "SONGS_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    first_name      VARCHAR,
    gender          VARCHAR,
    item_in_session INTEGER, 
    last_name       VARCHAR,
    length          DECIMAL,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    DECIMAL,
    session_id      INTEGER NOT NULL,
    song            VARCHAR,
    status          INTEGER,
    ts              BIGINT,
    user_agent      VARCHAR,
    user_id         VARCHAR
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id        VARCHAR NOT NULL, 
    artist_latitude  DECIMAL,
    artist_location  VARCHAR,
    artist_longitude DECIMAL,
    artist_name      VARCHAR,
    duration         DECIMAL,
    num_songs        INTEGER,
    song_id          VARCHAR NOT NULL,
    title            VARCHAR,
    year             INTEGER
);
"""

songplay_table_create = """
CREATE TABLE songplays (
    songplay_id INTEGER IDENTITY(0,1),
    start_time  TIMESTAMP NOT NULL,
    user_id     VARCHAR,
    level       VARCHAR,
    song_id     VARCHAR NOT NULL,
    artist_id   VARCHAR NOT NULL,
    session_id  INTEGER NOT NULL,
    location    VARCHAR,
    user_agent  VARCHAR,
    UNIQUE (start_time, user_id, session_id),
    PRIMARY KEY (songplay_id)
);
"""

user_table_create = """
CREATE TABLE users (
    user_id    VARCHAR,
    first_name VARCHAR,
    last_name  VARCHAR,
    gender     VARCHAR,
    level      VARCHAR,
    PRIMARY KEY (user_id)
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR,
    title       VARCHAR,
    artist_id   VARCHAR NOT NULL,
    year        INTEGER,
    duration    DECIMAL,
    PRIMARY KEY (song_id)
);
"""

artist_table_create = """
CREATE TABLE artists (
    artist_id VARCHAR,
    name      VARCHAR,
    location  VARCHAR,
    lattitude DECIMAL,
    longitude DECIMAL，
    PRIMARY KEY （artist_id)
)
"""

time_table_create = """
CREATE TABLE time(
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

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role={}'
format as json {}
region 'us-west-2';
"""
).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
format as json 'auto'
region 'us-west-2';
"""
).format(SONG_DATA, ARN)

# FINAL TABLES
## ts (bigint) => start_time (timestamp): https://knowledge.udacity.com/questions/154533
songplay_table_insert = """
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + (se.ts / 1000) * interval '1 second',
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
FROM staging_events se
JOIN staging_songs ss
ON (se.artist = ss.artist_name)
WHERE se.page = 'NextSong';
"""

user_table_insert = """
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT 
    se.user_id,
    se.first_name,
    se.last_name,
    se.gender,
    se.level
FROM staging_events se
WHERE se.page = 'NextSong';
"""

song_table_insert = """
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    ss.song_id,
    ss.title,
    ss.artist_id,
    CASE WHEN ss.year != 0 THEN ss.year
    ELSE null END AS year,
    ss.duration
FROM staging_songs ss;
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT  DISTINCT ss.artist_id,
        ss.artist_name,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
FROM staging_songs ss;
"""

time_table_insert = """
INSERT INTO time (start_time,hour,day,week,month,year,weekday)
SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second',
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time) ,
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(week FROM start_time)
FROM    staging_events se
WHERE se.page = 'NextSong';
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
