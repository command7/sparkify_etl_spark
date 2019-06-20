import configparser
from pyspark.sql import SparkSession
import os

conf_parser = configparser.ConfigParser()
with open('etl_config.cfg', 'r') as config_file:
    conf_parser.read_file(config_file)

aws_access = conf_parser['AWS']['AWS_ACCESS_KEY']
aws_secret = conf_parser['AWS']['AWS_SECRET_KEY']

spark = SparkSession.builder.config("spark.jars.packages",
                                    "org.apache.hadoop:hadoop-aws:2.7.0")\
    .getOrCreate()

log_data = spark.read.json(path='log-data/*')
song_data = spark.read.json(path='song_data/*/*/*/*.json')
log_data.show()
song_data.show()


def initiate_session():
    spark = SparkSession.builder\
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.0")\
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
    output_dir = os.path.join(output_location, "songs.parquet")
    song_data.write.parquet(output_dir)


def main():
    spark = initiate_session()
    songs_location = ""
    logs_location = ""
    songs_df, log_df = load_data(spark, songs_location, logs_location)


