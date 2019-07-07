import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#Create Schema
create_schema="CREATE SCHEMA IF NOT EXISTS Sparkify;"
set_schema="SET search_path TO Sparkify;"

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS Sparkify.stg_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS Sparkify.stg_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS Sparkify.songplays;"
user_table_drop = "DROP TABLE IF EXISTS Sparkify.users;"
song_table_drop = "DROP TABLE IF EXISTS Sparkify.songs;"
artist_table_drop = "DROP TABLE IF EXISTS Sparkify.artists;"
time_table_drop = "DROP TABLE IF EXISTS Sparkify.time;"

# CREATE TABLES
'''
stg_events and stg_songs :  These are staging tables. Initially data will be loaded into these tables from Amazon S3 bucket, which is having raw JSON files.
Not able to determine the diststyle for these two staging tables. Thats why I have selected diststyle as even ,rows are always evenly distributed across slices
'''
staging_events_table_create= ("""
CREATE TABLE dwh.Sparkify.stg_events (
artist varchar(255) encode text255, 
auth varchar(255) encode text255, 
firstName varchar(100),
gender varchar(1), 
itemInSession integer,
lastName varchar(100),
length DOUBLE PRECISION,
level varchar(20),
location varchar(255) encode text255,
method varchar(10),
page varchar(50),
registration varchar(100),
sessionId integer,
song varchar(200),
status integer,
ts bigint,
userAgent varchar(255) encode text255,
userId integer)
diststyle even;
""")

staging_songs_table_create = ("""
CREATE TABLE dwh.Sparkify.stg_songs (
artist_id varchar(100),
artist_latitude real,
artist_location varchar(255) encode text255,
artist_longitude real,
artist_name varchar(255) encode text255,
duration real,
num_songs integer,
song_id varchar(100),
title varchar(255) encode text255,
year integer)
diststyle even;
""")


'''
songplays is FACT table, thats why distribution key is defined on sp_song_id
sortkey has been defined in sp_start_time
column encoding is on sp_location and sp_user_agent as these two are varchar(256).
'''

songplay_table_create = ("""
CREATE TABLE dwh.Sparkify.songplays (
sp_id integer identity(1,1), 
sp_start_time bigint NOT NULL sortkey, 
sp_user_id integer ,
sp_level varchar(20) NOT NULL,
sp_song_id varchar(100) NOT NULL distkey,
sp_artist_id varchar(100) ,
sp_session_id integer, 
sp_location varchar(255) encode text255,
sp_user_agent varchar(255) encode text255,
sp_length DOUBLE PRECISION)
""")

'''
users is small table, thats why distribution key is diststyle all
sortkey has been defined in u_user_id
column encoding is not required as all are small text not big text/varchar.
'''

user_table_create = ("""
CREATE TABLE dwh.Sparkify.users(
u_user_id integer NOT NULL sortkey, 
u_first_name varchar(100) NOT NULL,
u_last_name varchar(100) NOT NULL,
u_gender varchar (1) NOT NULL,
u_level varchar(20) NOT NULL)
diststyle all;
""")

'''
artist is small table, thats why distribution key is diststyle all
sortkey has been defined in t_ts
column encoding is not required as all are integer.
'''

time_table_create = ("""
CREATE TABLE dwh.Sparkify.time(
t_ts bigint not null sortkey,
t_start_time timestamp NOT NULL,
t_hour integer,
t_day integer,
t_week integer,
t_month integer,
t_year integer,
t_weekday integer)
diststyle all;
""")

'''
songs table is big table, thats why distribution key is s_song_id column and sort order by s_title
column encoding has been done on s_title as it is big text.
'''

song_table_create = ("""
CREATE TABLE dwh.Sparkify.songs (
s_song_id varchar(100) NOT NULL distkey, 
s_title varchar(255) NOT NULL sortkey encode text255,
s_artist_id varchar(100) NOT NULL, 
s_year integer, 
s_duration real )
""")

'''
artist is small table, thats why distribution key is diststyle all
sortkey has been defined in a_artist_id
column encoding has been done on a_name and a_location
'''

artist_table_create = ("""
CREATE TABLE dwh.Sparkify.artists(
a_artist_id varchar(100) NOT NULL sortkey,
a_name varchar(255) NOT NULL encode text255,
a_location varchar(255) encode text255,
a_lattitude real, 
a_longitude real)
diststyle all;
""")

# STAGING TABLES
'''
Loading the staging tables from s3 buckets.
used copy mothod for first load
'''
staging_events_copy = ("""
SET search_path TO Sparkify;
copy stg_events from 's3://udacity-dend/log_data/' 
iam_role {}
json 's3://udacity-dend/log_json_path.json';
""").format(*config['IAM_ROLE'].values())
#print(staging_events_copy)
#json 's3://udacity-dend/log_data/2018/11/' 

staging_songs_copy = ("""
SET search_path TO Sparkify;
copy stg_songs from 's3://udacity-dend/song_data/' 
iam_role {}
json 'auto';
""").format(*config['IAM_ROLE'].values())
#print(staging_songs_copy)


# FINAL TABLES
'''
Fact and Dimesnion tables are loaded from staging tables.
'''

song_table_insert = ("""
INSERT INTO dwh.Sparkify.songs (s_song_id,s_title,s_artist_id,s_year,s_duration)
SELECT distinct song_id,title,artist_id,year,duration 
FROM Sparkify.stg_songs 
""")

artist_table_insert = ("""
INSERT INTO dwh.Sparkify.artists(a_artist_id,a_name,a_location,a_lattitude,a_longitude)
SELECT distinct artist_id,artist_name,artist_location,artist_latitude,artist_longitude   
FROM Sparkify.stg_songs
""")

songplay_table_insert = ("""
INSERT INTO dwh.Sparkify.songplays (sp_start_time,sp_user_id,sp_level,sp_song_id,sp_artist_id,sp_session_id,sp_location,sp_user_agent,sp_length)
SELECT distinct ts,userid,level,sg.song_id,ar.artist_id,sessionid,location,useragent,length 
FROM Sparkify.stg_events as ev  
inner join Sparkify.stg_songs as sg
on ev.song=sg.title
inner join Sparkify.stg_songs as ar
on ev.artist=ar.artist_name
""")

user_table_insert = ("""
INSERT INTO dwh.Sparkify.users(u_user_id,u_first_name,u_last_name,u_gender,u_level)
SELECT distinct userid,firstname,lastname,gender,level 
FROM Sparkify.stg_events  
where length(userid) > 0
""")

'''
SELECT distinct ev.userid,ev.firstname,ev.lastname,ev.gender
FROM Sparkify.stg_events  as ev
LEFT JOIN Sparkify.Users as us
on us.u_user_id=ev.userid
where us.u_user_id is null
AND LENGTH(ev.userid)>0
'''

time_table_insert = ("""
INSERT INTO dwh.Sparkify.time(t_ts,t_start_time,t_hour,t_day,t_week,t_month,t_year,t_weekday)
Select distinct ts
,t_start_time
,EXTRACT(HOUR FROM t_start_time) As t_hour
,EXTRACT(DAY FROM t_start_time) As t_day
,EXTRACT(WEEK FROM t_start_time) As t_week
,EXTRACT(MONTH FROM t_start_time) As t_month 
,EXTRACT(YEAR FROM t_start_time) As t_year 
,EXTRACT(DOW FROM t_start_time) As t_weekday 
FROM (
    SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as t_start_time
    FROM Sparkify.stg_events 
  ) tab
  
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
schema_queries =[create_schema,set_schema]

