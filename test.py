import time
import argparse
import logging
import sqlite3
import psycopg2

from properties import Properties
from opengauss_thread import OpenGaussThread
from connection import OpenGaussConnection, SqliteConnection


def main():

    logging.basicConfig(filename='database.log',
                        level=logging.DEBUG, filemode='a',
                        format='[%(asctime)s] [%(levelname)s] >>>  %(message)s',
                        datefmt='%Y-%m-%d %I:%M:%S')

    parser = argparse.ArgumentParser(description='Data Migration Script')
    parser.add_argument("--opengauss", "-o", default="")
    parser.add_argument("--sqlite", "-s", default="")

    args = parser.parse_args()

    opengauss_properties = {}
    is_file_update = False
    if args.opengauss != '':
        opengauss_file = str(args.opengauss)
        p = Properties(opengauss_file)
        opengauss_properties = p.get_properties()
    else:
        opengauss_file = 'opengauss.properties'
    if not opengauss_properties.__contains__('database.name') or opengauss_properties['database.name'] == '':
        opengauss_properties['database.name'] = input("Input the database name of OpenGauss:")
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
        p = Properties(sqlite_file)
        sqlite_properties = p.get_properties()
    else:
        sqlite_file = 'sqlite.properties'
    if not sqlite_properties.__contains__('database.filename'):
        sqlite_properties['database.filename'] = input("Input the filename of Sqlite3:")
        is_file_update = True
    if is_file_update:
        save_message = "Save your input in the %s? [y/n]" % sqlite_file
        flag = input(save_message)
        if flag.upper() == 'Y' or flag.upper() == 'YES':
            Properties.write_properties(sqlite_file, sqlite_properties)

    opengauss = OpenGaussConnection(opengauss_properties)
    sqlite = SqliteConnection(sqlite_properties)

    conn_sqlite = sqlite.getconn()
    cursor_sqlite = conn_sqlite.cursor()

    dbname = opengauss_properties['database.name']
    dbusername = opengauss_properties['database.user']
    conn_opengauss = opengauss.getconn()
    cursor_opengauss = conn_opengauss.cursor()
    try:
        cursor_opengauss.execute("create schema %s authorization %s;" % (dbname, dbusername))
        cursor_opengauss.execute("grant usage on schema %s to %s;" % (dbname, dbusername))
        cursor_opengauss.execute("set search_path to %s;" % dbname)
        conn_opengauss.commit()
        cursor_opengauss.close()
        opengauss.putconn(conn_opengauss)
    except psycopg2.errors.DuplicateSchema as e:
        logging.error(e)
        print("schema '%s' already exists" % dbname)
        cursor_opengauss.close()
        opengauss.putconn(conn_opengauss)
        conn_opengauss = opengauss.getconn()
        cursor_opengauss = conn_opengauss.cursor()
        cursor_opengauss.execute("grant usage on schema %s to %s;" % (dbname, dbusername))
        cursor_opengauss.execute("set search_path to %s;" % dbname)
        conn_opengauss.commit()
        cursor_opengauss.close()
        opengauss.putconn(conn_opengauss)

    """TODO: 数据迁移操作"""


if __name__ == '__main__':
    main()