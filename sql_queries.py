import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
S3_LOG_DATA = config.get('S3','LOG_DATA')
S3_SONG_DATA = config.get('S3','SONG_DATA')
IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

## Staging Tables: 
### Note that staging tables are used to store data raw/directly from S3 temporarely. 
### From there, we are loading data to the STAR schemas (Fact and dimension tables).
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (   
        event_id        BIGINT IDENTITY(0,1)    NOT NULL,
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          VARCHAR,
        itemInSession   INT,
        lastName        VARCHAR,
        length          NUMERIC,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    NUMERIC,
        sessionId       INT,
        song            VARCHAR,
        status          INT,
        ts              BIGINT,
        userAgent       VARCHAR,
        userId          INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs           INT,
        artist_id           VARCHAR,
        artist_latitude     NUMERIC,
        artist_longitude    NUMERIC,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            NUMERIC,
        year                INT
    );
""")

## Fact Table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id   INT       IDENTITY(0,1) PRIMARY KEY, 
        start_time    TIMESTAMP REFERENCES time(start_time)     NOT NULL, 
        userId       INT       REFERENCES users(userId)       NOT NULL, 
        level         VARCHAR, 
        song_id       VARCHAR   REFERENCES songs(song_id), 
        artist_id     VARCHAR   REFERENCES artists(artist_id), 
        session_id    INT, 
        location      VARCHAR, 
        user_agent    VARCHAR
    );
""")

## Dimension Tables
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        userId     INT        PRIMARY KEY, 
        firstName  VARCHAR    NOT NULL, 
        lastName   VARCHAR    NOT NULL, 
        gender      VARCHAR, 
        level       VARCHAR   
    );
""")

song_table_create = ("""
   CREATE TABLE IF NOT EXISTS songs
    (
        song_id     VARCHAR        PRIMARY KEY, 
        title       VARCHAR        NOT NULL, 
        artist_id   VARCHAR        NOT NULL, 
        year        INT, 
        duration    NUMERIC        NOT NULL
    );
""")

artist_table_create = ("""
   CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   VARCHAR       PRIMARY KEY, 
        name        VARCHAR       NOT NULL, 
        location    VARCHAR, 
        latitude    NUMERIC, 
        longitude   NUMERIC
    );
""")

time_table_create = ("""
   CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP   PRIMARY KEY, 
        hour        INT, 
        day         INT, 
        week        INT, 
        month       INT, 
        year        INT, 
        weekday     VARCHAR
    );
""")


# STAGING TABLES

staging_events_copy = (f"""
        COPY staging_events FROM '{S3_LOG_DATA}'
        CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
        REGION 'us-west-2' 
        COMPUPDATE OFF
        JSON AS '{S3_LOG_JSONPATH}'
    """)


staging_songs_copy = (f"""
        COPY staging_songs from '{S3_SONG_DATA}'
        CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
        REGION 'us-west-2' 
        COMPUPDATE OFF
        JSON AS 'auto'
    """)



# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays 
    (
        start_time,
        userId,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    ) 
    SELECT
        (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') AS start_time, 
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_songs ss
    INNER JOIN staging_events se
        ON (ss.title = se.song AND se.artist = ss.artist_name)
    AND se.page = 'NextSong';
    ;
""")
 

user_table_insert = ("""
INSERT INTO users 
    (
        userId, 
        firstName, 
        lastName, 
        gender, 
        level
    )
SELECT 
    DISTINCT userId, 
    firstName, 
    lastName, 
    gender, 
    level
FROM staging_events
WHERE userId IS NOT NULL AND page='NextSong'
""") 

song_table_insert = ("""
INSERT INTO songs 
    (
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    )
SELECT 
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists 
    (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    )
SELECT
    DISTINCT artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time 
    (   
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
SELECT 
    DISTINCT start_time, 
    DATE_PART(hr, start_time), 
    DATE_PART(d, start_time), 
    DATE_PART(w, start_time),
    DATE_PART(mon, start_time),
    DATE_PART(y, start_time), 
    DATE_PART(dow,start_time)
FROM songplays
WHERE start_time IS NOT NULL
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
