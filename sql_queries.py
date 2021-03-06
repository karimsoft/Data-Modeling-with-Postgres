# DROP TABLES

songplay_table_drop = "DROP table  IF EXISTS songplays"
user_table_drop = "DROP table  IF EXISTS users"
song_table_drop = "DROP table  IF EXISTS songs"
artist_table_drop ="DROP table  IF EXISTS artists"
time_table_drop = "DROP table  IF EXISTS time"


# CREATE TABLES 

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays ( songplay_id SERIAL PRIMARY KEY, \
                                                                time_id bigint NOT NULL REFERENCES time(time_id),\
                                                                user_id int NOT NULL REFERENCES users(user_id),\
                                                                level varchar,\
                                                                song_id varchar  NOT NULL REFERENCES  songs(song_id),  \
                                                                artist_id varchar  NOT NULL REFERENCES artists(artist_id),\
                                                                session_id int,location varchar,user_agent varchar)")

user_table_create = ("CREATE TABLE IF NOT EXISTS users ( user_id int PRIMARY KEY,\
                                                        first_name varchar, last_name varchar,\
                                                        gender varchar, level varchar)")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs ( song_id varchar PRIMARY KEY, \
                                                        title varchar, artist_id varchar, \
                                                        year int, duration numeric)")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists  ( artist_id varchar PRIMARY KEY,\
                                                            name varchar,location varchar,\
                                                            latitude float(7),longitude float(7))")

time_table_create = ("CREATE TABLE IF NOT EXISTS time ( time_id bigint PRIMARY KEY,\
                                                        start_time TIMESTAMP  NOT NULL ,\
                                                        hour int,day int,week int,month int,year int,weekday int)")

# INSERT RECORDS

songplay_table_insert = ("insert into songplays (time_id, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)")

user_table_insert = ("insert into users (user_id, first_name, last_name, gender, level) VALUES(%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING")

song_table_insert = ("insert into songs (song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s)")

artist_table_insert = ("insert into artists (artist_id, name, location, latitude, longitude) VALUES(%s, %s, %s, %s, %s)")

time_table_insert = ("insert into time (time_id,start_time, hour, day, week, month, year, weekday) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)")

# FIND SONGS

song_select = ("SELECT songs.song_id, artists.artist_id \
                FROM songs JOIN artists \
                    ON songs.artist_id=artists.artist_id\
                WHERE songs.title=(%s) AND artists.name=(%s) AND songs.duration=(%s)")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]