import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
                artist      VARCHAR                 NULL,
                auth        VARCHAR                 NULL,
                first_name   VARCHAR                 NULL,
                gender      VARCHAR                 NULL,
                item_in_session INTEGER               NULL,
                last_name    VARCHAR                 NULL,
                length      FLOAT                 NULL,
                level       VARCHAR                 NULL,
                location    VARCHAR                 NULL,
                method      VARCHAR                 NULL,
                page        VARCHAR                 NULL,
                registration FLOAT                NULL,
                session_id   INTEGER                 NOT NULL,
                song        VARCHAR                 NULL,
                status      INTEGER                 NULL,
                ts          TIMESTAMP                NOT NULL,
                user_agent   VARCHAR                 NULL,
                user_id      INTEGER                 NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
                artist_id           VARCHAR         NOT NULL,
                artist_latitude     FLOAT         NULL,
                artist_longitude    FLOAT         NULL,
                artist_location     VARCHAR         NULL,
                artist_name         VARCHAR         NULL,
                duration            FLOAT      NULL,
                num_songs           INTEGER         NULL,
                song_id             VARCHAR         NOT NULL,
                title               VARCHAR        NULL,
                year                INTEGER         NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER             NOT NULL        sortkey,
                first_name  VARCHAR             NULL,
                last_name   VARCHAR             NULL,
                gender      VARCHAR             NULL,
                level       VARCHAR             NULL,
                PRIMARY KEY(user_id)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
                song_id     VARCHAR         NOT NULL        sortkey,
                title       VARCHAR         NOT NULL,
                artist_id   VARCHAR         NOT NULL,
                year        INTEGER         NOT NULL,
                duration    FLOAT         NOT NULL,
                PRIMARY KEY (song_id),
                FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
                artist_id   VARCHAR             NOT NULL          sortkey,
                name        VARCHAR           NULL,
                location    VARCHAR           NULL,
                latitude    NUMERIC              NULL,
                longitude   NUMERIC              NULL,
                PRIMARY KEY (artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
                start_time  TIMESTAMP               NOT NULL       sortkey    distkey,
                hour        SMALLINT                NULL,
                day         SMALLINT                NULL,
                week        SMALLINT                NULL,
                month       SMALLINT                NULL,
                year        SMALLINT                NULL,
                weekday     SMALLINT                NULL,
                PRIMARY KEY(start_time)
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
                songplay_id INTEGER IDENTITY(0,1)   NOT NULL,
                start_time  TIMESTAMP               NOT NULL    sortkey    distkey,
                user_id     INTEGER             NOT NULL,
                level       VARCHAR             NOT NULL,
                song_id     VARCHAR             NOT NULL,
                artist_id   VARCHAR             NOT NULL,
                session_id  INTEGER             NOT NULL,
                location    VARCHAR            NULL,
                user_agent  VARCHAR            NULL,
                PRIMARY KEY (songplay_id),
                FOREIGN KEY(start_time) REFERENCES time(start_time),
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(song_id) REFERENCES songs(song_id),
                FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    );
""")
# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON {}
    REGION 'us-west-2'
    TIMEFORMAT 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT se.ts,
                se.user_id,
                se.level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.session_id,
                se.location,
                se.user_agent
FROM staging_events se
JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id,
                first_name,
                last_name,
                gender,
                level
FROM staging_events se
WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
                artist_name ,
                artist_location,
                artist_latitude,
                artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
                EXTRACT(HOUR FROM ts),
                EXTRACT(DAY FROM ts),
                EXTRACT(WEEK FROM ts),
                EXTRACT(MONTH FROM ts),
                EXTRACT(YEAR FROM ts),
                EXTRACT(DOW FROM ts)
FROM staging_events
WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
