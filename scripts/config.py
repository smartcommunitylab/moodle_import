from configparser import ConfigParser

def get_params(type):
    parameters = {}
    parser = ConfigParser()
    parser.read("config.ini")
    if parser.has_section(type):
        parameters = {
            item[0] : item[1]
            for item in parser.items(type)
        }
    return parameters

instance_params = get_params("moodle_local")

                