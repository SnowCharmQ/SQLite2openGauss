import os
import argparse
import logging
from lib import multi_thread, single_thread
from prop.properties import Properties


def main():
    fmt = logging.Formatter(fmt='[%(asctime)s] [%(levelname)s] >>>  %(message)s', datefmt='%Y-%m-%d %I:%M:%S')
    if not os.path.exists("log"):
        os.mkdir("log")
    if not os.path.exists("sqlite"):
        os.mkdir("sqlite")
    file1 = logging.FileHandler(filename='log/error.log', mode='a', encoding='utf-8')
    file1.setFormatter(fmt)
    error_log = logging.Logger(name='ERROR_LOG', level=logging.ERROR)
    error_log.addHandler(file1)
    file2 = logging.FileHandler(filename='log/info.log', mode='a', encoding='utf-8')
    file2.setFormatter(fmt)
    info_log = logging.Logger(name='INFO_LOG', level=logging.INFO)
    info_log.addHandler(file2)

    parser = argparse.ArgumentParser(description='Data Migration Script')
    parser.add_argument("--opengauss", "-o", default="")
    parser.add_argument("--sqlite", "-s", default="")
    parser.add_argument("--multithreading", "-m", action="store_true")

    args = parser.parse_args()

    opengauss_properties = {}
    is_file_update = False
    if args.opengauss != '':
        opengauss_file = 'prop/' + str(args.opengauss)
        p = Properties(opengauss_file)
        opengauss_properties = p.get_properties()
    else:
        opengauss_file = 'prop/opengauss.properties'
    if not opengauss_properties.__contains__('database.name') or opengauss_properties['database.name'] == '':
        opengauss_properties['database.name'] = input("Input the database name of OpenGauss:")
        is_file_update = True
    if not opengauss_properties.__contains__('database.schema') or opengauss_properties['database.schema'] == '':
        opengauss_properties['database.schema'] = input("Input the schema name of OpenGauss:")
        is_file_update = True
    if not opengauss_properties.__contains__('database.host') or opengauss_properties['database.host'] == '':
        opengauss_properties['database.host'] = input("Input the host of OpenGauss:")
        is_file_update = True
    if not opengauss_properties.__contains__('database.port') or opengauss_properties['database.port'] == '':
        opengauss_properties['database.port'] = input("Input the port of OpenGauss:")
        is_file_update = True
    if not opengauss_properties.__contains__('database.user') or opengauss_properties['database.user'] == '':
        opengauss_properties['database.user'] = input("Input the username of OpenGauss:")
        is_file_update = True
    if not opengauss_properties.__contains__('database.password') or opengauss_properties['database.password'] == '':
        opengauss_properties['database.password'] = input("Input the user password of OpenGauss:")
        is_file_update = True
    if is_file_update:
        save_message = "Save your input in the %s? [y/n]" % opengauss_file
        flag = input(save_message)
        if flag.upper() == 'Y' or flag.upper() == 'YES':
            Properties.write_properties(opengauss_file, opengauss_properties)

    sqlite_properties = {}
    is_file_update = False
    if args.sqlite != '':
        sqlite_file = str(args.sqlite)
        sqlite_file = "prop/" + sqlite_file
        p = Properties(sqlite_file)
        sqlite_properties = p.get_properties()
    else:
        sqlite_file = 'prop/sqlite.properties'
    if not sqlite_properties.__contains__('database.filename'):
        sqlite_properties['database.filename'] = input("Input the filename of Sqlite3:")
        is_file_update = True
    if is_file_update:
        save_message = "Save your input in the %s? [y/n]" % sqlite_file
        flag = input(save_message)
        if flag.upper() == 'Y' or flag.upper() == 'YES':
            Properties.write_properties(sqlite_file, sqlite_properties)

    sqls_log = None
    flag = input("Save the SQL statements in Data Migration? [y/n]")
    is_record_sqls = False
    if flag.upper() == 'Y' or flag.upper() == 'YES':
        is_record_sqls = True
        file3 = logging.FileHandler(filename='log/sqls.log', mode='a', encoding='utf-8')
        file3.setFormatter(fmt)
        sqls_log = logging.Logger(name='SQLS_LOG', level=logging.DEBUG)
        sqls_log.addHandler(file3)

    if args.multithreading:
        multi_thread.multi_thread(opengauss_properties, sqlite_properties, error_log, info_log, sqls_log,
                                  is_record_sqls)
    else:
        single_thread.single_thread(opengauss_properties, sqlite_properties, error_log, info_log, sqls_log,
                                    is_record_sqls)


if __name__ == '__main__':
    main()
