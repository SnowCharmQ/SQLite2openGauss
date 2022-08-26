import codecs
import psycopg2
import sqlite3
import sys


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
    sql += ';'
    while True:
        return sql

dbname = 'postgres'

try:
    conn_opengauss = psycopg2.connect(database=dbname,
                                      user="gaussdb",
                                      password="sustechD1!",
                                      host="localhost",
                                      port="15432")
except psycopg2.OperationalError:
    print("Fail to connect to openGauss database")
    exit(1)

try:
    conn_sqlite = sqlite3.connect('sqlite/filmdb.sqlite')
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
final_str_list=[]

for row in all_table:
        print(row[4])
        s = row[4]
        s=s.replace('\n', '').replace('\r', '')
        print(s)




length=len(final_str_list)
origin=sys.stdout
file_path='test1.txt'
sys.stdout = open(file_path, "w",encoding='utf8')
count=1
for sql in conn_sqlite.iterdump():
    if sql.find("CREATE") !=-1:
        s = sql.split(' ')
        location = s[2].find('(')
        if (location != -1):
            final_str = s[2][0:location].replace('\n', '').replace('\r', '')
            final_str_list.append([final_str, 0, 0])
        else:
            final_str = s[2].replace('\n', '').replace('\r', '')
        sql = convert_datetime_to_date(sql)
        for i in range(0,length):
            if (final_str_list[i][0]==final_str):
                final_str_list[i][1] = count
    count=count+1
    print(sql.replace('\n', '').replace('\r', ''))

sys.stdout=origin
for i in range(0,length):
    print(final_str_list[i][0],final_str_list[i][1])

file_path_out='out1.txt'
sys.stdout = open(file_path_out, "w",encoding='utf8')
with codecs.open(file_path, 'r', encoding='utf8') as infile:
    for count in range(0,length):
        print(count)
        boo=0
        for sql in infile.readlines()[final_str_list[count][1]-1:]:
            if(sql.find("CREATE") !=-1):
                if(boo==0):
                    sql=sql.replace('\n', '').replace('\r', '')
                    print(sql)
                    cursor_opengauss.execute(sql)
                    boo=1
                else:
                    break
            else:
                print(sql.replace('\n', '').replace('\r', ''))
                cursor_opengauss.execute(sql.replace('\n', '').replace('\r', ''))
        infile.seek(0)
        conn_opengauss.commit()



