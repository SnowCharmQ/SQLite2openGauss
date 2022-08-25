import argparse
import logging
import psycopg2
import decorator
import time

from prop.properties import Properties
from opengauss_thread import OpenGaussThread, OpenGaussLogThread
from connection import OpenGaussConnection, SqliteConnection


def main():
    fmt = logging.Formatter(fmt='[%(asctime)s] [%(levelname)s] >>>  %(message)s', datefmt='%Y-%m-%d %I:%M:%S')
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

    opengauss = OpenGaussConnection(opengauss_properties, error_log, info_log)
    sqlite = SqliteConnection(sqlite_properties, error_log, info_log)

    conn_sqlite = sqlite.getconn()

    dbusername = opengauss_properties['database.user']
    dbschema = opengauss_properties['database.schema']
    conn_opengauss = opengauss.getconn()
    cursor_opengauss = conn_opengauss.cursor()
    try:
        cursor_opengauss.execute("create schema %s authorization %s;" % (dbschema, dbusername))
        cursor_opengauss.execute("grant usage on schema %s to %s;" % (dbschema, dbusername))
        conn_opengauss.commit()
        cursor_opengauss.close()
        opengauss.putconn(conn_opengauss)
    except psycopg2.errors.DuplicateSchema as e:
        info_log.info(e)
        cursor_opengauss.close()
        opengauss.putconn(conn_opengauss)

    time_start = time.time()

    cursor_sqlite = conn_sqlite.cursor()
    all_table = cursor_sqlite.execute("select * from sqlite_master where type = 'table';")
    create_sqls = []
    for row in all_table:
        s = row[4]
        s = s.replace('\n', '').replace('\r', '').replace('   ', ' ')
        create_sqls.append(s + ";")
    try:
        conn_opengauss = opengauss.getconn()
        cursor_opengauss = conn_opengauss.cursor()
        cursor_opengauss.execute("set search_path to %s;" % dbschema)
        for sql in create_sqls:
            if sql.find("CREATE") != -1:
                sql = decorator.createWithoutFK(sql)
                cursor_opengauss.execute(sql)
            else:
                sql = decorator.Insert(sql)
                cursor_opengauss.execute(sql)
            if is_record_sqls:
                sqls_log.info(sql)
        conn_opengauss.commit()
    except Exception as e:
        error_log.error(e)
    finally:
        if conn_opengauss is not None:
            opengauss.putconn(conn_opengauss)

    count = 0
    sqls = []
    thread_list = []
    create_sqls = []
    for sql in conn_sqlite.iterdump():
        if sql.find("CREATE") != -1:
            create_sqls.append(sql)
            continue
        sqls.append(sql)
        count += 1
        if count == 100:
            if is_record_sqls:
                t = OpenGaussLogThread(opengauss, sqls, dbschema, error_log, sqls_log)
            else:
                t = OpenGaussThread(opengauss, sqls, dbschema, error_log)
            thread_list.append(t)
            t.start()
            sqls = []
            count = 0
    if is_record_sqls:
        t = OpenGaussLogThread(opengauss, sqls, dbschema, error_log, sqls_log)
    else:
        t = OpenGaussThread(opengauss, sqls, dbschema, error_log)
    thread_list.append(t)
    t.start()
    for t in thread_list:
        t.join()

    try:
        conn_opengauss = opengauss.getconn()
        cursor_opengauss = conn_opengauss.cursor()
        cursor_opengauss.execute("set search_path to %s;" % dbschema)
        for create_sql in create_sqls:
            sqls = decorator.alterFK(create_sql)
            for alter_sql in sqls:
                cursor_opengauss.execute(alter_sql)
            sqls_log.info(create_sql)
        conn_opengauss.commit()
    except Exception as e:
        error_log.error(e)
    finally:
        if conn_opengauss is not None:
            opengauss.putconn(conn_opengauss)

    time_end = time.time()

    time_c = time_end - time_start
    print('Time Cost =', time_c, 'seconds')


if __name__ == '__main__':
    main()
