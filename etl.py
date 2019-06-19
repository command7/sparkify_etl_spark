import configparser

conf_parser = configparser.ConfigParser()
conf_parser.read_file(open('credentials.cfg', 'r'))

print(conf_parser['AWS']['AWS_SECRET_KEY'])