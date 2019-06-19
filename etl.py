import configparser
from pyspark.sql import SparkSession

conf_parser = configparser.ConfigParser()
with open('etl_config.cfg', 'r') as config_file:
    conf_parser.read_file(config_file)

aws_access = conf_parser['AWS']['AWS_ACCESS_KEY']
aws_secret = conf_parser['AWS']['AWS_SECRET_KEY']

