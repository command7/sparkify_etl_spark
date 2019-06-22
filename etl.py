import configparser
from pyspark.sql import SparkSession
import os


def initiate_session():
    spark = SparkSession.builder\
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.6")\
        .appName("sparkify_etl")\
        .getOrCreate()
    return spark


def load_songs_data(spark_session, input_location):
    songs_df = spark_session.read.json(input_location)
    return songs_df


def load_log_data(spark_session, input_location):
    logs_df = spark_session.read.json(input_location)
    return logs_df


def load_data(spark_session, songs_location, logs_location):
    songs_df = load_songs_data(spark_session, songs_location)
    logs_df = load_log_data(spark_session, logs_location)
    return songs_df, logs_df


def create_temp_table(data_frame, table_name):
    data_frame.createOrReplaceTempView(table_name)


def etl_songs_table(spark_session, output_location):
    extract_song_data = """
    SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM song_data
    """
    song_data = spark_session.sql(extract_song_data)
    output_dir = os.path.join(output_location, "songs.parquet")
    song_data.write.parquet(output_dir)
    print("Songs ETL Completed.")


def etl_users_table(spark_session, output_location):
    extract_user_data = """
    SELECT DISTINCT userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM log_data
    """
    users_data = spark_session.sql(extract_user_data)
    output_dir = os.path.join(output_location, "users.parquet")
    users_data.write.parquet(output_dir)
    print("Users ETL Completed.")


def etl_artists_table(spark_session, output_location):
    extract_artists_data = """
    SELECT DISTINCT artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS lattitude,
        artist_longitude AS longitude
    FROM song_data
    """
    artists_data = spark_session.sql(extract_artists_data)
    output_dir = os.path.join(output_location, "artists.parquet")
    artists_data.write.parquet(output_dir)
    print("Artists ETL Completed.")


def etl_time_table(spark_session, output_location):
    extract_time_data = """
    SELECT CAST(t1.timestamp_temp AS TIMESTAMP) AS start_time,
        HOUR(t1.timestamp_temp) AS hour,
        DAYOFMONTH(t1.timestamp_temp) AS day,
        WEEKOFYEAR(t1.timestamp_temp) AS week,
        MONTH(t1.timestamp_temp) AS month,
        YEAR(t1.timestamp_temp) AS year,
        CASE WHEN DAYOFWEEK(t1.timestamp_temp) IN (6, 7) THEN True ELSE False 
        END AS 
        weekday
    FROM 
        (SELECT FROM_UNIXTIME(ts/1000, 'YYYY-MM-dd hh:mm:ss') AS 
        timestamp_temp 
        FROM log_data) t1
    """
    time_data = spark_session.sql(extract_time_data)
    output_dir = os.path.join(output_location, "time.parquet")
    time_data.write.parquet(output_dir)
    print("Time ETL Completed.")


def etl_songsplay_table(spark_session, output_location):
    extract_songsplay_data = """
    SELECT CONCAT(log_temp.ts, log_temp.user_id) AS songplay_id,
        CAST(log_temp.start_time AS TIMESTAMP) AS start_time,
        log_temp.user_id,
        log_temp.level,
        song_temp.song_id,
        song_temp.artist_id,
        log_temp.session_id,
        log_temp.location,
        log_temp.user_agent
    FROM (SELECT FROM_UNIXTIME(ts/1000, 'YYYY-MM-dd hh:mm:ss') AS start_time,
            ts,
            userId AS user_id,
            level,
            song,
            artist,
            location,
            sessionId AS session_id,
            userAgent AS user_agent
        FROM log_data) log_temp
    JOIN
        (SELECT song_id,
            artist_id,
            artist_name,
            title
        FROM song_data) song_temp
    ON log_temp.song = song_temp.title
    AND log_temp.artist = song_temp.artist_name
    """
    songsplay_data = spark_session.sql(extract_songsplay_data)
    output_dir = os.path.join(output_location, "songsplay.parquet")
    songsplay_data.write.parquet(output_dir)
    print("Songs play ETL Completed.")


def run_etl(spark_session, output_location):
    etl_users_table(spark_session, output_location)
    etl_artists_table(spark_session, output_location)
    etl_songs_table(spark_session, output_location)
    etl_time_table(spark_session, output_location)
    etl_songsplay_table(spark_session, output_location)


def main():
    conf_parser = configparser.ConfigParser()
    conf_parser.read_file(open("aws/credentials.cfg", "r"))
    AWS_ACCESS_KEY_ID = conf_parser['AWS']['AWS_ACCESS_KEY']
    AWS_SECRET_ACCESS_KEY = conf_parser['AWS']['AWS_SECRET_KEY']
    spark = initiate_session()
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",
                                                      AWS_ACCESS_KEY_ID)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",
                                                      AWS_SECRET_ACCESS_KEY)
    songs_location = "song_data/*/*/*/*.json"
    logs_location = "log-data/*"
    output_dir = conf_parser["AWS"]["OUTPUT_PATH"]
    songs_df, logs_df = load_data(spark, songs_location, logs_location)
    create_temp_table(songs_df, "song_data")
    create_temp_table(logs_df, "log_data")
    try:
        run_etl(spark, output_dir)
        print("ETL Completed")
    except Exception as e:
        print(e)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
