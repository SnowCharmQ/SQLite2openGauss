import time
import psycopg2

from lib import decorator2
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
        s = s.replace('\n', '').replace('\r', '').replace('   ', ' ')
        create_sqls.append(s + ";")
    try:
        conn_opengauss = opengauss.getconn()
        cursor_opengauss = conn_opengauss.cursor()
        cursor_opengauss.execute("set search_path to %s;" % dbschema)
        tables = []
        dic = {}
        for sql in create_sqls:
            if sql.upper().startswith("CREATE"):

                index = sql.find('(')
                table_name = sql[13:index]
                newsql = decorator2.createWithoutFK(sql)

                if sql.find("AUTOINCREMENT") != -1 or sql.find("autoincrement") != -1:
                    newsql, col = decorator2.autoIncrement(newsql)
                    tables.append(table_name)
                    dic[table_name] = col

                cursor_opengauss.execute(newsql)

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
    for sql in conn_sqlite.iterdump():
        if sql.upper().startswith("CREATE"):
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
            sqls = decorator2.alterFK(create_sql)
            for alter_sql in sqls:
                cursor_opengauss.execute(alter_sql)
                if is_record_sqls:
                    sqls_log.info(alter_sql)
        for t, c in dic.items():
            row_num = cursor_sqlite.execute("SELECT COUNT(*) FROM " + t)
            seq_sql = "CREATE SEQUENCE sq_" + t + "  START " + row_num + " INCREMENT 1 CACHE 20;"
            cursor_opengauss.execute(seq_sql)  # 创建自增序列
            alter_sql2 = "ALTER TABLE " + t + " ALTER COLUMN " + c + " set default nextval('sq_" + t + "');"
            cursor_opengauss.execute(alter_sql2)
            sqls_log.info(seq_sql)
            sqls_log.info(alter_sql2)
        conn_opengauss.commit()
    except Exception as e:
        error_log.error(e)
    finally:
        if conn_opengauss is not None:
            opengauss.putconn(conn_opengauss)

    time_end = time.time()

    time_c = time_end - time_start
    print('Time Cost = %.2f seconds' % time_c)