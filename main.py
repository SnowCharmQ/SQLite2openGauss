import psycopg2
import sqlite3


def convert_datetime_to_date(sql: str):
    return sql.replace("datetime", "date")


def try_to_remove(sql: str, x: int):
    ll = x
    rr = x
    while sql[ll] != ',':
        ll -= 1
    while sql[rr] != ',' and sql[rr] != ';':
        rr += 1
    if sql[rr] == ',':
        fk = sql[ll:rr]
    else:
        fk = sql[ll:(rr - 1)]
    sql = sql.replace(fk, '')
    return sql


def remove_foreign_key(sql: str):
    ss = sql.split('\n')
    map(lambda x: x.strip(), ss)
    sql = "".join(ss)
    while True:
        x = sql.find("foreign key")
        if x == -1:
            return sql
        else:
            sql = try_to_remove(sql, x)


dbname = 'olin'

try:
    conn_opengauss = psycopg2.connect(database=dbname,
                                      user="olin",
                                      password="test123@",
                                      host="120.46.202.38",
                                      port="26000")
except psycopg2.OperationalError:
    print("Fail to connect to openGauss database")
    exit(1)

try:
    conn_sqlite = sqlite3.connect('filmdb.sqlite')
except BaseException:
    print("Fail to connect to sqlite database")
    exit(1)

cursor_opengauss = conn_opengauss.cursor()
cursor_sqlite = conn_sqlite.cursor()

all_table = cursor_sqlite.execute("select * from sqlite_master where type = 'table';")

try:
    cursor_opengauss.execute("create schema %s;" % dbname)
except psycopg2.errors.DuplicateSchema:
    print("schema '%s' already exists" % dbname)
    conn_opengauss.commit()


cursor_opengauss.execute("start transaction;")
cursor_opengauss.execute("set search_path to %s;" % dbname)

# create_sql_list = []
table_list = []

for row in all_table:
    # try:
        # sql = remove_foreign_key(row[4])
        # sql = convert_datetime_to_date(sql)
        # create_sql_list.append(sql)
    table_name = row[1]
    table_list.append(table_name)
    # except (
    #         psycopg2.errors.DuplicateTable, psycopg2.errors.FeatureNotSupported,
    #         psycopg2.errors.InFailedSqlTransaction, psycopg2.errors.UndefinedObject):
    #     print("Error!!!!!!!!!")

# table_count = len(create_sql_list)
# for i in range(table_count):
    # cursor_opengauss.execute("set search_path to %s;" % dbname)
    # create_sql = create_sql_list[i]
    # table_name = table_list[i]
    # select_sql = "select * from %s;" % table_name
    # cursor_opengauss.execute(create_sql)
    # table_data = cursor_sqlite.execute(select_sql)
    # for row in table_data:
        # insert_sql = "insert into %s values %s;" % (table_name, str(row))
        # cursor_opengauss.execute(insert_sql)
    # print(select_column_sql)
    # columns = map(lambda x: x[3], table_columns)
    # print(columns)
for sql in conn_sqlite.iterdump():
    if sql.find("CREATE TABLE") != -1:
        sql = remove_foreign_key(sql)
        sql = convert_datetime_to_date(sql)
    print(sql)
    cursor_opengauss.execute(sql)

conn_opengauss.commit()
