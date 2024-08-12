import os
from configparser import ConfigParser

def config(filename='database.ini', section='postgresql'):
    """
    Считывает конфигурацию базы данных из указанного файла.

    Эта функция читает файл конфигурации (по умолчанию 'database.ini')
    и возвращает параметры подключения к базе данных в виде словаря.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    filepath = os.path.join(project_root, filename)

    print(f"Attempting to read config file: {filepath}")

    # создание парсера
    parser = ConfigParser()
    # четние конфигнутого файла
    parser.read(filepath)

    print(f"Sections found in the config file: {parser.sections()}")

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filepath} file')

    return db


