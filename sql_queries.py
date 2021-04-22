import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_DB = config.get("DWH","DWH_DB")
DWH_DB_USER = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")


S3_LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
S3_LOG_DATA = config.get('S3','LOG_DATA')
S3_SONG_DATA = config.get('S3','SONG_DATA')
IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

## Staging Tables: 
### Note that staging tables are used to store data raw/directly from S3 temporarely. 
### From there, we are loading data to the STAR schemas (Fact and dimension tables).
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_event_table
    (
        artist          text,
        auth            text,
        firstName       text,
        gender          text,
        iteminSession   int,
        lastName        text,
        length          float,
        level           text,
        location        text,
        method          text,
        page            text,
        registration    float,
        sessionId       int,
        song            text,
        status          int,
        ts              float,
        userAgent       text,
        userId          int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_song_table
    (
        num_songs           int,
        artist_id           text,
        artist_latitude     float,
        artist_longitude    float,
        artist_location     text,
        artist_name         text,
        song_id             text,
        title               text,
        duration            float,
        year                int
    );
""")

## Fact Table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id   BIGSERIAL PRIMARY KEY, 
        start_time    TIMESTAMP REFERENCES time(start_time), 
        user_id       int       NOT NULL    REFERENCES users(user_id), 
        level         text, 
        song_id       text      REFERENCES songs(song_id), 
        artist_id     varchar   REFERENCES artists(artist_id), 
        session_id    int, 
        location      text, 
        user_agent    text
    );
""")

## Dimension Tables
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id     int     PRIMARY KEY, 
        first_name  text    NOT NULL, 
        last_name   text    NOT NULL, 
        gender      text, 
        level       text   
    );
""")

song_table_create = ("""
   CREATE TABLE IF NOT EXISTS songs
    (
        song_id     text        PRIMARY KEY, 
        title       text        NOT NULL, 
        artist_id   varchar     NOT NULL, 
        year        int, 
        duration    float       NOT NULL
    );
""")

artist_table_create = ("""
   CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   varchar     PRIMARY KEY, 
        name        text        NOT NULL, 
        location    text, 
        latitude    float, 
        longitude   float
    );
""")

time_table_create = ("""
   CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP   PRIMARY KEY, 
        hour        int, 
        day         int, 
        week        int, 
        month       int, 
        year        int, 
        weekday     text
    );
""")


# STAGING TABLES

staging_events_copy = (f"""
        copy staging_events from '{S3_LOG_DATA}'
        credentials 'aws_iam_role={IAM_ROLE_ARN}'
        region 'us-west-2' 
        COMPUPDATE OFF STATUPDATE OFF
        JSON '{S3_LOG_JSONPATH}'
    """)


staging_songs_copy = (f"""
        copy staging_songs from '{S3_SONG_DATA}'
        credentials 'aws_iam_role={IAM_ROLE_ARN}'
        region 'us-west-2' 
        COMPUPDATE OFF STATUPDATE OFF
        JSON '{S3_LOG_JSONPATH}'
    """)



# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays 
        (songplay_id, start_time, user_id, level, song_id, artist_id, 
        session_id, location, user_agent) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""
    INSERT INTO users 
        (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")   

song_table_insert = ("""
    INSERT INTO songs 
        (song_id, title, artist_id, year, duration)    
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists 
        (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
    INSERT INTO time 
        (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
