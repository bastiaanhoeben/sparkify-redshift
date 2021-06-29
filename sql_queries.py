import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
    artist VARCHAR
    auth VARCHAR
    firstName VARCHAR 
    gender VARCHAR(1)
    ItemInSession INT
    lastName VARCHAR
    lenght FLOAT
    level VARCHAR
    location VARCHAR
    method VARCHAR
    page VARCHAR
    registration FLOAT
    sessionId INT
    song VARCHAR
    status INT
    ts INT
    userAgent VARCHAR
    userID INT
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INT
    artist_id VARCHAR
    artist_latitude FLOAT
    artist_longitude FLOAT
    artist_location VARCHAR
    artist_name VARCHAR
    song_id VARCHAR
    title VARCHAR
    duration FLOAT
    year INT
)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id IDENTITY(0,1) PRIMARY KEY
    start_time INT FOREIGN KEY REFERENCES time (start_time)
    user_id INT FOREIGN KEY REFERENCES users (user_id)
    level VARCHAR
    song_id VARCHAR FOREIGN KEY REFERENCES songs (song_id)
    artist_id VARCHAR FOREIGN KEY REFERENCES artists (artist_id)
    session_id INT
    location VARCHAR FOREIGN KEY REFERENCES artists (location)
    user_agent VARCHAR 
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id INT PRIMARY KEY
    first_name VARCHAR
    last_name VARCHAR
    gender VARCHAR(1)
    level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY
    title VARCHAR
    artist_id VARCHAR FOREIGN KEY REFERENCES artists (artist_id)
    year INT
    duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY
    name VARCHAR
    location VARCHAR
    lattitude FLOAT
    longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY
    hour INT
    day INT
    week INT
    month INT
    year INT
    weekday BOOLEAN
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY {} FROM 's3://udacity-dend/log_data'
CREDENTIALS 'aws_iam_role={}'
json region 'us-west-2'
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
