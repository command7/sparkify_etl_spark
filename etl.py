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


def create_temp_table(data_frame, table_name):
    data_frame.createOrReplaceTempView(table_name)


def etl_songs_table(spark_session, songs_df, output_location):
    extract_song_data = """
    SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM song_data
    """
    song_data = spark.sql(extract_song_data)
    output_dir = os.path.join(output_location, "songs.parquet")
    song_data.write.parquet(output_location)

