import psycopg2
import pandas as pd

import create_tables
from sql_queries import (
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    song_select,
)
from utils import build_df_from_single_filepath, get_files


def process_artist_data(cursor, df):
    """Filter a subset of df in order to match
    the artists schema, then insert the filtered rows
    into artists table."""

    # insert artist record
    artist_data = df[
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ].dropna(axis=0, subset=["artist_id"])
    if len(artist_data) > 0:
        for _, row in artist_data.iterrows():
            cursor.execute(artist_table_insert, row)


def process_song_data(cursor, df):
    """Filter a subset of df in order to match
    the songs schema, then insert the filtered rows
    into songs table."""

    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].dropna(
        axis=0, subset=["song_id"]
    )
    if len(song_data) > 0:
        for _, row in song_data.iterrows():
            cursor.execute(song_table_insert, row)


def process_time_data(cursor, df):
    """Convert unix epoch into datetime, then calculate
    time parameters to match the time schema, then insert
    the data into time table."""

    if len(df) > 0:
        # convert timestamp column to datetime
        timestamp = pd.to_datetime(df["ts"], unit="ms").values
        hour = pd.to_datetime(df["ts"], unit="ms").dt.hour.values
        day = pd.to_datetime(df["ts"], unit="ms").dt.day.values
        week = pd.to_datetime(df["ts"], unit="ms").dt.isocalendar().week.values
        month = pd.to_datetime(df["ts"], unit="ms").dt.month.values
        year = pd.to_datetime(df["ts"], unit="ms").dt.year.values
        weekday = pd.to_datetime(df["ts"], unit="ms").dt.weekday.values

        # insert time record into time table
        time_data = (timestamp, hour, day, week, month, year, weekday)
        column_labels = (
            "start_time",
            "hour",
            "day",
            "week",
            "month",
            "year",
            "weekday",
        )
        time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

        for _, row in time_df.iterrows():
            cursor.execute(time_table_insert, row)


def process_user_data(cursor, df):
    """Filter a subset of df in order to match
    the users schema, then insert the filtered rows
    into users table."""

    user_df = df[
        [
            "userId",
            "firstName",
            "lastName",
            "gender",
            "level",
        ]
    ].dropna(axis=0, subset=["userId"])

    # insert user records
    if len(user_df) > 0:
        user_df["userId"] = user_df["userId"].astype(int)
        for _, row in user_df.iterrows():
            cursor.execute(user_table_insert, row)


def process_songplay_data(cursor, df):
    """Iterate over each row of df, execute query song_select to
    retrieve songid and artistd from songs and artists tables, then
    insert a songplay_data record into songplay table."""

    if len(df) > 0:
        for _, row in df.iterrows():

            # get songid and artistid from song and artist tables
            cursor.execute(song_select, (row.song, row.artist, row.length))
            results = cursor.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # insert songplay record
            timestamp = pd.to_datetime(row.ts, unit="ms")
            songplay_data = (
                timestamp,
                row.userId,
                row.level,
                songid,
                artistid,
                row.sessionId,
                row.location,
                row.userAgent,
            )
            cursor.execute(songplay_table_insert, songplay_data)


def process_song_file(cursor, filepath):
    """Open a .json file from filepath and insert a record
    into artists and songs tables."""

    # open song file
    df = build_df_from_single_filepath(filepath=filepath)

    # insert artist record into artists table
    process_artist_data(cursor=cursor, df=df)

    # insert song record into songs table
    process_song_data(cursor=cursor, df=df)


def process_log_file(cursor, filepath):
    """Open a .json file from filepath and insert a record
    into time, users and songplay tables."""

    # open log file
    df = build_df_from_single_filepath(filepath=filepath)

    # filter data by NextSong action
    df = df[df["page"] == "NextSong"]
    df.reset_index(inplace=True, drop=True)

    # insert time record into time table
    process_time_data(cursor=cursor, df=df)

    # insert user record into users table
    process_user_data(cursor=cursor, df=df)

    # insert songplay record into songplay table
    process_songplay_data(cursor=cursor, df=df)


def process_data(cursor, connection, filepath, func):
    """Retrieve all files from filepath, process thoses
    files with func, then with connection and cursor insert
    data into the database."""
    # get all files matching extension from directory
    all_files = get_files(filepath=filepath)

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cursor, datafile)
        connection.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    # drop sparkifydb database if exists, then re-create it.
    create_tables.main()

    connection = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cursor = connection.cursor()

    process_data(cursor, connection, filepath="data/song_data", func=process_song_file)
    process_data(cursor, connection, filepath="data/log_data", func=process_log_file)

    connection.close()


if __name__ == "__main__":
    main()