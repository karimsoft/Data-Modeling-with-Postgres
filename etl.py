import os
import glob
import psycopg2
import numpy as np
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
   """Process json song file from filepath
    
    Arguments:
        cur {cursor} -- psycopg2 cursor
        filepath {str} -- path to json file containing song data
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    try:
        cur.execute(song_table_insert, song_data)
    except:
        pass
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    try:
        cur.execute(artist_table_insert, artist_data)
    except:
        pass


def process_log_file(cur, filepath):
     """
    Processes log file from log_data directory to create three tables: time, users, and songplays 
    
    Args:
    - cur: Allows to run Postgres command
    - filepath: File to be processed and extracted to Postgres tables
    
    Returns: None
    """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df=df[df['page']=='NextSong']
    
    # convert timestamp column to datetime
    t =  [df.ts,pd.to_datetime(df.ts, unit='ms')]
    
    # insert time data records
    time_data = np.transpose(np.array([t[0], t[1].dt.strftime('%Y-%m-%d %H:%M:%S'), t[1].dt.hour, t[1].dt.day, t[1].dt.week, t[1].dt.month, t[1].dt.year, t[1].dt.weekday]))
    column_labels = ('time_id','timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday')
    time_df =  pd.DataFrame(data = time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId,row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()