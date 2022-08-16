import argparse
import logging
import sqlite3
import psycopg2

import properties

parser = argparse.ArgumentParser(description='Data Migration Script')
parser.add_argument("--opengauss", "-o", default="")
parser.add_argument("--sqlite", "-s", default="")

args = parser.parse_args()

opengauss_properties = {}
is_file_update = False
if args.opengauss != '':
    opengauss_file = str(args.opengauss)
    p = properties.Properties(opengauss_file)
    opengauss_properties = p.get_properties()
else:
    opengauss_file = 'opengauss.properties'
if not opengauss_properties.__contains__('database.name'):
    opengauss_properties['database.name'] = input("Input the database name of OpenGauss:")
    is_file_update = True
if not opengauss_properties.__contains__('database.host'):
    opengauss_properties['database.host'] = input("Input the host of OpenGauss:")
    is_file_update = True
if not opengauss_properties.__contains__('database.port'):
    opengauss_properties['database.port'] = input("Input the port of OpenGauss:")
    is_file_update = True
if not opengauss_properties.__contains__('database.user'):
    opengauss_properties['database.user'] = input("Input the username of OpenGauss:")
    is_file_update = True
if not opengauss_properties.__contains__('database.password'):
    opengauss_properties['database.password'] = input("Input the user password of OpenGauss:")
    is_file_update = True
if is_file_update:
    save_message = "Save your input in the %s? [y/n]" % opengauss_file
    flag = input(save_message)
    if flag.upper() == 'Y' or flag.upper() == 'YES':
        properties.Properties.write_properties(opengauss_file, opengauss_properties)
try:
    conn_opengauss = psycopg2.connect(database=opengauss_properties['database.name'],
                                      user=opengauss_properties['database.user'],
                                      password=opengauss_properties['database.password'],
                                      host=opengauss_properties['database.host'],
                                      port=opengauss_properties['database.port'])
    logging.info(
        "Successfully Log In %s As %s" % (opengauss_properties['database.name'], opengauss_properties['database.user']))
except psycopg2.OperationalError as e:
    print("Fail to connect to openGauss database")
    logging.error(e)
    raise e

sqlite_properties = {}
is_file_update = False
if args.sqlite != '':
    sqlite_file = str(args.sqlite)
    p = properties.Properties(sqlite_file)
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
        properties.Properties.write_properties(sqlite_file, sqlite_properties)
try:
    conn_sqlite = sqlite3.connect(sqlite_properties['database.filename'])
except BaseException as e:
    print("Fail to connect to sqlite database")
    logging.error(e)
    raise e
