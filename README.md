## Overview
Sparkify, a  music streaming startup, is scaling up and wants to move to cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
<img width="893" height="543" alt="image" src="https://github.com/user-attachments/assets/28eb22a2-65dd-4204-b5cb-c14aeed9b9a9" />

As shown in the picture above, their data contains songs (song_data) and user activity (log_data) that should be ETLed to Redshift for analytics purpose. A growing music startup may want to:
   - Motivate/reward best performing artists on the platform
   - Motivate/reward active users
   - Build a recommendation system for users to propose songs or artists thereby enhancing user experience
   - Find out if there are usage gaps. When do people use the app less? Which time of day? Which days of week? Which month? Which year? Some periods might show a spike/decrease in usage which might correlate to new song releases or updates in the app. This could help track relationships between updates in the application and impact on usage

## How to execute the project
The repository contains 4 mains files
   - **sql_queries.py** - contains all queries for creating and loading the tables
   - **create_tables.py** - running this script allows to create all tables
   - **etl.py** - running this script allows to load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift
   - **dwh.cfg** - contains S3 and Redshift configurations

To execute the project:
1. Launch a redshift cluster and create an IAM role that has read access to S3.
2. Add redshift database and IAM role info to dwh.cfg
3. Run create_tables.py - this will create staging and analytical tables
4. Run etl.py - this will load data into all tables
5. Execute the analytical queries (see analytical queries section below) using the Query Editor in your AWS Redshift console

create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.
etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.

## Database schema and ETL
The database is made of a fact table (songplays) around which there are 4 dimension tables (time, artists, songs and users)
<img width="912" height="644" alt="image" src="https://github.com/user-attachments/assets/a76ca7f9-8242-4ab9-bfa7-13364c1a4a2e" />

**Songplays**
   - Records in event data associated with song plays.
   - It has user_id as a distribution key and start_time as a sortkey allowing for efficient user activity analysis over time.
   - The data for this table is extracted by fetching information from both staging_events and staging_songs tables creating a link using song titles and artist names.

**Users**
   - Users in the app
   - Since the number of users is small, this table to fully copied across all notes with an ALL distribution style.
   - User information is extracted from the staging_events table

**Songs** 
   - Songs in music database
   - Naturally distributed on song_id for efficient retrieval of each song using song_id
   - Song information is extracted from the staging_songs table

**Artists**
  - Artists in music database
  - Naturally distributed on artists_id for efficient retrieval of each artist using artist_id
  - Artist information is extracted from the staging_songs table

**Time**
  - Timestamps of records in song plays broken down into specific units
  - Uses start_time as both a sortkey and distkey for efficient time-based analysis.
  - Times are extracted from the staging_events table and duplicates are removed
	
## Counts per table
| Table | Count |
| :------- | :------: |
| staging_events     | 8056 |
| staging_songs | 14896   |
| artists   | 10025   |
| songplays   | 6820   |
| songs   | 14896   |
| users   | 105   |
| time   | 6813   |

## Analytical queries

**Top 10 songs**
```
WITH distinct_artist AS (
  SELECT artist_id, name FROM 
  (SELECT artist_id, name, ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY name DESC) AS rn
   FROM artists)
  WHERE rn = 1
)
SELECT songplays.song_id, title, name, COUNT(*) as total_listened FROM
songplays
JOIN songs ON songplays.song_id = songs.song_id
JOIN distinct_artist ON songs.artist_id = distinct_artist.artist_id
GROUP BY songplays.song_id, title, name
ORDER BY total_listened DESC
LIMIT 10
```
<img width="1363" height="636" alt="image" src="https://github.com/user-attachments/assets/6ecf1a08-0c94-4f4a-b537-deb4e180e3e5" />

**Top 10 active users**
```
SELECT songplays.user_id, first_name, last_name, COUNT(*) as total_activity
FROM songplays
JOIN users ON songplays.user_id = users.user_id
GROUP BY songplays.user_id, first_name, last_name
ORDER BY total_activity DESC
LIMIT 10
```
<img width="1362" height="625" alt="image" src="https://github.com/user-attachments/assets/58526161-1634-47c6-8e32-a9ecfd046319" />

**Usage by time**
```
SELECT weekday, hour, COUNT(*) AS total_usage
FROM songplays
JOIN time ON songplays.start_time = time.start_time
GROUP BY GROUPING SETS ((time.weekday), (time.hour))
ORDER BY total_usage DESC
```
<img width="601" height="361" alt="image" src="https://github.com/user-attachments/assets/9343efdd-e30f-4536-bd03-c3627c6c5964" />
<img width="630" height="360" alt="image" src="https://github.com/user-attachments/assets/cdd4545f-4b97-40e3-910c-3b74a48457bf" />
