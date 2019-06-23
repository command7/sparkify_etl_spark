# Project Title

## Description

A fictional music streaming company namely *Sparkify* has data about its 
songs, artists of 
those songs and other relevant information in the form of JSON files. It 
also stores information about when songs are played, by whom they are played
 and other relevant logging data. These JSON files are stored in an AWS S3 
 bucket. 
 
 The program `etl.py` uses _Apache Spark_ to read these JSON files from the S3 
 bucket, 
 transforms the data into a star schema after denormalizing and stores 
 them as _parquet_ files. 
 
 ## How to run
 
 In order to run this program, you need to create an AWS user with 
 privileges to access data from S3 and write data to S3. The `ACCESS KEY` 
 and `SECRET KEY` should be stored in a `.cfg` file at `aws/credentials.cfg'.
 
 An example of how the config file looks is as follows
 
 ```
 [AWS]
 AWS_ACCESS_KEY = ABC
AWS_SECRET_KEY = XYZ
```

Once credentials are stored in the specified file, you can initiate the ETL 
workflow by using
`python3 etl.py`

## Initial Structure of Data
### Songs JSON Schema

* num_songs : 1
* artist_id : ARJIE2Y1187B994AB7
* artist_latitude : null
* artist_longitude : null
* artist_location : _
* artist_name : Line Renaud
* song_id : SOUPIRU12A6D4FA1E1
* title : Der Kleine Dompfaff
* duration : 152.92036
* year : 0

### Log JSON Schema 

![Image](https://github.com/command7/sparkify_etl_spark/blob/master/Images/log-data.png)

## Star Schema

`Fact Table`

* songplays - records in log data associated with song plays i.e. records 
with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

`Dimension Tables`

* users - users in the app
user_id, first_name, last_name, gender, level
* songs - songs in music database
song_id, title, artist_id, year, duration
* artists - artists in music database
artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

