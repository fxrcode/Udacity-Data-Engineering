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


staging_events_table_create= ("""
    CREATE TABLE public.staging_events (
    	artist varchar(256),
    	auth varchar(256),
    	firstName varchar(256),
    	gender varchar(256),
    	itemInSession int4,
    	lastName varchar(256),
    	length numeric(18,0),
    	"level" varchar(256),
    	location varchar(256),
    	"method" varchar(256),
    	page varchar(256),
    	registration numeric(18,0),
    	sessionId int4,
    	song varchar(256),
    	status int4,
    	ts int8,
    	userAgent varchar(256),
    	userId int4
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE public.staging_songs (
    	num_songs int4,
    	artist_id varchar(256),
    	artist_name varchar(256),
    	artist_latitude numeric(18,0),
    	artist_longitude numeric(18,0),
    	artist_location varchar(256),
    	song_id varchar(256),
    	title varchar(256),
    	duration numeric(18,0),
    	"year" int4
    );
""")

songplays_table_create = ("""
    CREATE TABLE public.songplays (
        playid varchar(32) NOT NULL,
        start_time timestamp NOT NULL,
        userid int4 NOT NULL,
        "level" varchar(256),
        songid varchar(256),
        artistid varchar(256),
        sessionid int4,
        location varchar(256),
        user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);
""")

users_table_create = ("""
    CREATE TABLE public.users (
    	user_id int4 NOT NULL,
    	first_name varchar(256),
    	last_name varchar(256),
    	gender varchar(256),
    	"level" varchar(256),
    	CONSTRAINT users_pkey PRIMARY KEY (user_id)
    );
""")

songs_table_create = ("""
    CREATE TABLE public.songs (
    	song_id varchar(256) NOT NULL,
    	title varchar(256),
    	artist_id varchar(256),
    	"year" int4,
    	duration numeric(18,0),
    	CONSTRAINT songs_pkey PRIMARY KEY (song_id)
    );
""")

artists_table_create = ("""
    CREATE TABLE public.artists (
    	artist_id varchar(256) NOT NULL,
    	name varchar(256),
    	location varchar(256),
    	latitude numeric(18,0),
    	longitude numeric(18,0)
    );
""")

time_table_create = ("""
    CREATE TABLE public."time" (
    	start_time timestamp NOT NULL,
    	"hour" int4,
    	"day" int4,
    	week int4,
    	"month" varchar(256),
    	"year" int4,
    	weekday varchar(256),
    	CONSTRAINT time_pkey PRIMARY KEY (start_time)
    ) ;
""")

# QUERY LISTS
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplays_table_create, users_table_create, songs_table_create, artists_table_create, time_table_create]
