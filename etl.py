import psycopg2
import pandas as pd
from sql_queries import *
from utils import build_df_from_single_filepath, get_files


def process_song_file(cur, filepath):
    # open song file
    df = build_df_from_single_filepath(filepath=filepath)

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
        for i, row in artist_data.iterrows():
            cur.execute(artist_table_insert, row)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].dropna(
        axis=0, subset=["song_id"]
    )
    if len(song_data) > 0:
        for i, row in song_data.iterrows():
            cur.execute(song_table_insert, row)


def process_log_file(cur, filepath):
    # open log file
    df = build_df_from_single_filepath(filepath=filepath)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]
    if len(df) > 0:
        df.reset_index(inplace=True, drop=True)

        # convert timestamp column to datetime
        timestamp = pd.to_datetime(df["ts"], unit="ms").values
        hour = pd.to_datetime(df["ts"], unit="ms").dt.hour.values
        day = pd.to_datetime(df["ts"], unit="ms").dt.day.values
        week = pd.to_datetime(df["ts"], unit="ms").dt.isocalendar().week.values
        month = pd.to_datetime(df["ts"], unit="ms").dt.month.values
        year = pd.to_datetime(df["ts"], unit="ms").dt.year.values
        weekday = pd.to_datetime(df["ts"], unit="ms").dt.weekday.values

        # insert time data records
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

        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, row)

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].dropna(
        axis=0, subset=["userId"]
    )

    # insert user records
    if len(user_df) > 0:
        user_df["userId"] = user_df["userId"].astype(int)
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)

    # insert songplay records
    if len(df) > 0:
        for index, row in df.iterrows():

            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

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
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = get_files(filepath=filepath)

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()