import configparser
from planet import api


config = configparser.ConfigParser()
config.read('config.ini')

for section in config:
    if section == "DEFAULT":
        continue
    pass
    # code to download data
