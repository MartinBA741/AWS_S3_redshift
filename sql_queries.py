import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event_teable"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_event_teable
    (
        
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_song_table
    (
        
    );
""")

# Fact Table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_table
    (
        songplay_id BIGSERIAL PRIMARY KEY, 
        start_time TIMESTAMP REFERENCES time(start_time), 
        user_id int NOT NULL REFERENCES users(user_id), 
        level text, 
        song_id text REFERENCES songs(song_id), 
        artist_id varchar REFERENCES artists(artist_id), 
        session_id int, 
        location text, 
        user_agent text
    );
""")

# Dimension Tables
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table
    (
        user_id int PRIMARY KEY, 
        first_name text NOT NULL, 
        last_name text NOT NULL, 
        gender text, 
        level text   
    );
""")

song_table_create = ("""
   CREATE TABLE IF NOT EXISTS song_table
    (
        song_id text PRIMARY KEY, 
        title text NOT NULL, 
        artist_id varchar NOT NULL, 
        year int, 
        duration float NOT NULL
    );
""")

artist_table_create = ("""
   CREATE TABLE IF NOT EXISTS artist_table
    (
        artist_id varchar PRIMARY KEY, 
        name text NOT NULL, 
        location text, 
        latitude float, 
        longitude float
    );
""")

time_table_create = ("""
   CREATE TABLE IF NOT EXISTS time_table
    (
        start_time TIMESTAMP PRIMARY KEY, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday text
    );
""")

# STAGING TABLES

staging_events_copy = ("""
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
