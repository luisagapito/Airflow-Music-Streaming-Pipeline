user_table_insert = ("""
SELECT distinct userid, firstname, lastname, gender, level
FROM staging_events
WHERE page='NextSong'
""")
    
song_table_insert = ("""
SELECT distinct song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")
    
time_table_insert = ("""
SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time, 
extract(hour from start_time) as hour_time,  
extract(day from start_time) as day_time, 
extract(week from start_time) as week_time,  
extract(month from start_time) as month_time,  
extract(year from start_time) as year_time,  
extract(weekday from start_time) as weekday_time  
from staging_events  
where page= 'NextSong'  
and start_time is not null;
""")