import configparser
from psycopg2 import sql


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier("users"))
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE staging_events (
  artist         varchar(255),
  auth           varchar(50),
  firstName      varchar(100),
  gender         char(1),
  itemInSession  integer,
  lastName       varchar(100),
  length         float,
  level          varchar(10),
  location       varchar(255),
  method         varchar(10),
  page           varchar(50),
  registration   bigint,
  sessionId      integer,
  song           varchar(255),
  status         integer,
  ts             bigint,
  userAgent      varchar(512),
  userId         integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
  artist_id         varchar(20),
  artist_latitude   float,
  artist_location   varchar(255),
  artist_longitude  float,
  artist_name       varchar(255),
  duration          float,
  num_songs         integer,
  song_id           varchar(20),
  title             varchar(255),
  year              integer
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
  songplay_id    bigint          not null identity(0,1),
  start_time     bigint          not null sortkey,
  user_id        integer         not null distkey,
  level          varchar(10)     not null,
  song_id        varchar(18),
  artist_id      varchar(18),
  session_id     integer         not null,
  location       varchar(255)    not null,
  user_agent     varchar(255)    not null)
""")

user_table_create = ("""
CREATE TABLE users (
  user_id      integer         not null,
  first_name   varchar(255)    not null,
  last_name    varchar(255)    not null,
  gender       char(1)         not null,
  level        varchar(10)     not null)
  diststyle all
""")

song_table_create = ("""
CREATE TABLE songs (
  song_id     varchar(20)     not null distkey,
  title       varchar(255)    not null,
  artist_id   varchar(20)     not null,
  year        integer         not null,
  duration    decimal         not null)
""")

artist_table_create = ("""
CREATE TABLE artists (
  artist_id    varchar(20)     not null distkey,
  name         varchar(255)    not null,
  location     varchar(255),
  latitude     decimal,
  longitude    decimal)
""")

time_table_create = ("""
CREATE TABLE time (
  start_time   bigint          not null distkey sortkey,
  hour         integer         not null,
  day          integer         not null,
  week         integer         not null,
  month        integer         not null,
  year         integer         not null,
  weekday      integer         not null)
""")

# STAGING TABLES
staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log_data' 
credentials 'aws_iam_role={role_arn}'
json 's3://udacity-dend/log_json_path.json'
REGION 'us-west-2';
""").format(role_arn=config.get('IAM_ROLE', 'ARN').strip("'"))


staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={role_arn}' 
json 'auto'
REGION 'us-west-2';
""").format(role_arn=config.get('IAM_ROLE', 'ARN').strip("'"))

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT ts AS start_time, userId AS user_id, level, song_id, artist_id, sessionId as session_id, location, userAgent as user_agent
FROM staging_events
LEFT JOIN staging_songs on song = title AND artist = artist_name
WHERE page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists  (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    ts AS start_time,
    EXTRACT(HOUR FROM TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') AS hour,
    EXTRACT(DAY FROM TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') AS day,
    EXTRACT(WEEK FROM TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') AS week,
    EXTRACT(MONTH FROM TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') AS month,
    EXTRACT(YEAR FROM TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') AS year,
    EXTRACT(DOW FROM TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') AS weekday
FROM staging_events
WHERE page = 'NextSong'
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
