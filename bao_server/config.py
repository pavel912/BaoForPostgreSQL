import configparser

def read_config():
    config = configparser.ConfigParser()
    config.read("BaoForPostgreSQL/bao_server/bao.cfg")

    if "bao" not in config:
        print("bao.cfg does not have a [bao] section.")
        exit(-1)

    config = config["bao"]
    return config
