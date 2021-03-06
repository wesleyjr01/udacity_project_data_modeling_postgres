# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays \
    (songplay_id SERIAL PRIMARY KEY, \
    start_time TIMESTAMP, \
    user_id INT NOT NULL, \
    level VARCHAR, \
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR,
    FOREIGN KEY (user_id)
        REFERENCES users (user_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (song_id)
        REFERENCES songs (song_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (artist_id)
        REFERENCES artists (artist_id)
        ON UPDATE CASCADE ON DELETE CASCADE);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users \
    (user_id INT PRIMARY KEY, \
    first_name VARCHAR, \
    last_name VARCHAR, \
    gender VARCHAR, \
    level VARCHAR);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs \
    (song_id VARCHAR PRIMARY KEY, \
    title VARCHAR, \
    artist_id VARCHAR NOT NULL, \
    year INT, \
    duration int, \
    FOREIGN KEY (artist_id)
        REFERENCES artists (artist_id)
        ON UPDATE CASCADE ON DELETE CASCADE);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists \
    (artist_id VARCHAR PRIMARY KEY, \
    name VARCHAR, \
    location VARCHAR, \
    latitude FLOAT, \
    longitude FLOAT);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time \
    (start_time TIMESTAMP PRIMARY KEY, \
    hour INT, \
    day INT, \
    week INT, \
    month INT, \
    year INT, \
    weekday INT);
"""

# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplays \
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
"""

user_table_insert = """
INSERT INTO users \
    (user_id, first_name, last_name, gender, level) \
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET \
    (first_name, last_name, gender, level) = \
        (EXCLUDED.first_name, EXCLUDED.last_name, EXCLUDED.gender, EXCLUDED.level)
"""

song_table_insert = """
INSERT INTO songs \
    (song_id, title, artist_id, year, duration) \
    VALUES(%s, %s, %s, %s, %s) \
    ON CONFLICT (song_id) DO NOTHING
"""

artist_table_insert = """
INSERT INTO artists \
    (artist_id, name, location, latitude, longitude) \
    VALUES(%s, %s, %s, %s, %s) \
    ON CONFLICT (artist_id) DO NOTHING
"""


time_table_insert = """
INSERT INTO time \
    (start_time, hour, day, week, month, year, weekday) \
    VALUES(%s, %s, %s, %s, %s, %s, %s) \
    ON CONFLICT (start_time) DO NOTHING
"""

# FIND SONGS

song_select = """
SELECT songs.song_id, artists.artist_id FROM songs \
    JOIN artists ON artists.artist_id = songs.artist_id \
    WHERE songs.title LIKE %s \
    AND artists.name LIKE %s \
    AND songs.duration = CAST(%s AS INTEGER);
"""

# QUERY LISTS

create_table_queries = [
    artist_table_create,
    user_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create,
]

drop_table_queries = [
    artist_table_drop,
    user_table_drop,
    song_table_drop,
    time_table_drop,
    songplay_table_drop,
]
