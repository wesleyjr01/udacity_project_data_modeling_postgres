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
    start_time INT, \
    user_id INT NOT NULL, \
    level VARCHAR, \
    song_id INT NOT NULL,
    artist_id INT NOT NULL,
    session_id INT NOT NULL,
    location INT,
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
    (user_id SERIAL PRIMARY KEY, \
    first_name VARCHAR, \
    last_name VARCHAR, \
    gender VARCHAR, \
    level VARCHAR);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs \
    (song_id SERIAL PRIMARY KEY, \
    title VARCHAR, \
    artist_id INT NOT NULL, \
    year INT, \
    duration INT, \
    FOREIGN KEY (artist_id)
        REFERENCES artists (artist_id)
        ON UPDATE CASCADE ON DELETE CASCADE);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists \
    (artist_id SERIAL PRIMARY KEY, \
    name VARCHAR, \
    location VARCHAR, \
    latitude INT, \
    longitude INT);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time \
    (start_time INT, \
    hour INT, \
    day INT, \
    week INT, \
    month INT, \
    year INT, \
    weekday INT);
"""

# INSERT RECORDS

songplay_table_insert = """
"""

user_table_insert = """
"""

song_table_insert = """
"""

artist_table_insert = """
"""


time_table_insert = """
"""

# FIND SONGS

song_select = """
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
