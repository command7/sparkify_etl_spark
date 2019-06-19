import configparser
from pyspark.sql import SparkSession

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

