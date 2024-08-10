import os
from configparser import ConfigParser


def config(filename='database.ini', section='postgresql'):
    # Get the absolute path of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up one directory to reach the project root
    project_root = os.path.dirname(script_dir)
    # Construct the full path to database.ini
    filepath = os.path.join(project_root, filename)

    print(f"Attempting to read config file: {filepath}")

    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filepath)

    print(f"Sections found in the config file: {parser.sections()}")

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filepath} file')

    return db
