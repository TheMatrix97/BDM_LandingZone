import configparser


def parse_properties(key):
    config = configparser.RawConfigParser()
    config.read('project.properties')
    return config[key]
