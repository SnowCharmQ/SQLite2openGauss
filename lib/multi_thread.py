import time
import psycopg2

from lib import decorator
from lib.connection import OpenGaussConnection, SqliteConnection
from lib.opengauss_thread import OpenGaussLogThread, OpenGaussThread


def multi_thread(opengauss_properties, sqlite_properties, error_log, info_log, sqls_log, is_record_sqls):
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

    print("The data migration operation is in progress...")
    time_start = time.time()

    cursor_sqlite = conn_sqlite.cursor()
    all_table = cursor_sqlite.execute("select * from sqlite_master where type = 'table';")
    create_sqls = []
    for row in all_table:
        s = row[4]
        create_sqls.append(s + ";")
    try:
        conn_opengauss = opengauss.getconn()
        cursor_opengauss = conn_opengauss.cursor()
        cursor_opengauss.execute("set search_path to %s;" % dbschema)
        for sql in create_sqls:
            sql = decorator.remove_comment(sql)
            sql = decorator.create_without_fk(sql)
            cursor_opengauss.execute(sql)
            if is_record_sqls:
                sqls_log.info(sql.replace("\n", ""))
        conn_opengauss.commit()
    except Exception as e:
        error_log.error(e)
    finally:
        if conn_opengauss is not None:
            opengauss.putconn(conn_opengauss)

    count = 0
    sqls = []
    thread_list = []
    for sql in conn_sqlite.iterdump():
        sql = decorator.remove_comment(sql)
        if sql.upper().startswith("CREATE"):
            continue
        sqls.append(sql)
        count += 1
        if count == 50:
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
            create_sql = decorator.remove_comment(create_sql)
            sqls = decorator.alter_fk(create_sql)
            for alter_sql in sqls:
                cursor_opengauss.execute(alter_sql)
                if is_record_sqls:
                    sqls_log.info(alter_sql.replace("\n", ""))
            table_name = decorator.get_table_name(create_sql)
            row_num = cursor_sqlite.execute("SELECT COUNT(*) FROM " + table_name)
            sqls = decorator.autoincrement(create_sql, table_name, row_num)
            for alter_sql in sqls:
                cursor_opengauss.execute(alter_sql)
                if is_record_sqls:
                    sqls_log.info(alter_sql.replace("\n", ""))
        conn_opengauss.commit()
    except Exception as e:
        error_log.error(e)
    finally:
        if conn_opengauss is not None:
            opengauss.putconn(conn_opengauss)

    try:
        conn_opengauss = opengauss.getconn()
        cursor_opengauss = conn_opengauss.cursor()
        cursor_opengauss.execute("set search_path to %s;" % dbschema)
        indexes = cursor_sqlite.execute("select * from sqlite_master where type = 'index' and sql is not null;")
        for row in indexes:
            sql = row[4] + ";"
            sql = decorator.remove_comment(sql)
            cursor_opengauss.execute(sql)
            if is_record_sqls:
                sqls_log.info(sql)
        conn_opengauss.commit()
    except Exception as e:
        error_log.error(e)
    finally:
        if conn_opengauss is not None:
            opengauss.putconn(conn_opengauss)

    try:
        conn_opengauss = opengauss.getconn()
        cursor_opengauss = conn_opengauss.cursor()
        cursor_opengauss.execute("set search_path to %s;" % dbschema)
        views = cursor_sqlite.execute("select * from sqlite_master where type = 'view';")
        for row in views:
            sql = row[4]
            sql = decorator.remove_comment(sql)
            cursor_opengauss.execute(sql)
            if is_record_sqls:
                sqls_log.info(sql)
        conn_opengauss.commit()
    except Exception as e:
        error_log.error(e)
    finally:
        if conn_opengauss is not None:
            opengauss.putconn(conn_opengauss)

    try:
        conn_opengauss = opengauss.getconn()
        cursor_opengauss = conn_opengauss.cursor()
        cursor_opengauss.execute("set search_path to %s;" % dbschema)
        triggers = cursor_sqlite.execute("select * from sqlite_master where type = 'trigger';")
        for row in triggers:
            trigger_name = row[1]
            trigger_sql = row[4]
            trigger_sql = decorator.remove_comment(trigger_sql)
            function = decorator.trigger_to_function(trigger_name, trigger_sql)
            function = decorator.remove_comment(function)
            trigger = decorator.new_trigger(trigger_name, trigger_sql)
            trigger = decorator.remove_comment(trigger)
            cursor_opengauss.execute(function)
            cursor_opengauss.execute(trigger)
            if is_record_sqls:
                sqls_log.info(function)
                sqls_log.info(trigger)
        conn_opengauss.commit()
    except Exception as e:
        error_log.error(e)
    finally:
        if conn_opengauss is not None:
            opengauss.putconn(conn_opengauss)

    time_end = time.time()

    time_c = time_end - time_start
    print('Time Cost = %.2f seconds' % time_c)
