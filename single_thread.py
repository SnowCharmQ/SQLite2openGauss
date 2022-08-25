import time
import psycopg2

import decorator
from connection import OpenGaussConnection, SqliteConnection


def single_thread(opengauss_properties, sqlite_properties, error_log, info_log, sqls_log, is_record_sqls):
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
        create_sqls = []
        for sql in conn_sqlite.iterdump():
            if sql.find("CREATE") != -1:
                create_sqls.append(sql)
                continue
            elif sql.find("BEGIN TRANSACTION;") != -1:
                continue
            else:
                sql = decorator.Insert(sql)
                cursor_opengauss.execute(sql)
                if is_record_sqls:
                    sqls_log.info(sql)
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