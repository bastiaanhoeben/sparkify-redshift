import configparser
from aws_config import get_role_arn


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_REGION, LOG_DATA, LOG_JSONPATH, SONG_DATA = config['S3'].values()
KEY, SECRET, REGION = config['AWS'].values()
IAM_ROLE_NAME = config.get('IAM_ROLE', 'IAM_ROLE_NAME')
role_arn = get_role_arn(REGION, KEY, SECRET, IAM_ROLE_NAME)

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
CREATE TABLE staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR(1),
    ItemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId VARCHAR
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL,
    PRIMARY KEY (songplay_id),
    FOREIGN KEY (start_time) REFERENCES time (start_time),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR(1) NOT NULL,
    level VARCHAR NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT NOT NULL,
    duration FLOAT NOT NULL,
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR,
    name VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    lattitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday BOOLEAN
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
IAM_ROLE '{}'
REGION '{}'
JSON {};
""").format(LOG_DATA, role_arn, S3_REGION, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE '{}'
REGION '{}'
JSON 'auto';
""").format(SONG_DATA, role_arn, S3_REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT DISTINCT
    TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second',
    e.userId,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId,
    e.location,
    e.userAgent
FROM staging_events AS e
    INNER JOIN staging_songs AS s
    ON e.artist = s.artist_name
    AND e.song = s.title
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
WITH cte AS
(
    SELECT 
        userId, 
        firstName, 
        lastName, 
        gender, 
        level,
        ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank
    FROM staging_events
    WHERE page = 'NextSong'
        AND userId IS NOT NULL
)
SELECT DISTINCT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM cte
WHERE rank = 1
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    lattitude,
    longitude
)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
WITH cte AS
(
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS clean_timestamp
    FROM staging_events
    WHERE page = 'NextSong'
)
SELECT DISTINCT
    clean_timestamp,
    EXTRACT(HOUR FROM clean_timestamp),
    EXTRACT(DAY FROM clean_timestamp),
    EXTRACT(WEEK FROM clean_timestamp),
    EXTRACT(MONTH FROM clean_timestamp),
    EXTRACT(YEAR FROM clean_timestamp),
    EXTRACT(DOW FROM clean_timestamp)
FROM cte
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, time_table_insert, song_table_insert, songplay_table_insert]
