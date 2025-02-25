# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender CHAR(1),
    item_in_session INTEGER,
    last_name VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    session_id INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    user_agent VARCHAR,
    user_id INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id INTEGER NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL DISTKEY,
    artist_id VARCHAR NOT NULL,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
);
""")

# STAGING TABLES COPY COMMANDS
staging_events_copy = ("""
COPY staging_events FROM 's3://your-bucket-name/log_data'
CREDENTIALS 'aws_iam_role=your-iam-role'
FORMAT AS JSON 's3://your-bucket-name/log_json_path.json'
REGION 'us-west-2';
""")

staging_songs_copy = ("""
COPY staging_songs FROM 's3://your-bucket-name/song_data'
CREDENTIALS 'aws_iam_role=your-iam-role'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""")

# INSERT RECORDS
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS start_time, 
       se.user_id, 
       se.level, 
       ss.song_id, 
       ss.artist_id, 
       se.session_id, 
       se.location, 
       se.user_agent
FROM staging_events se
JOIN staging_songs ss
ON se.song = ss.title
AND se.artist = ss.artist_name
AND se.length = ss.duration
WHERE se.page = 'NextSong';
""")
