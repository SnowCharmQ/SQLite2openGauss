import sqlite3
import time

time_start = time.time()

try:
    conn = sqlite3.connect('test.sqlite')  # position of sqlite file
    print("Successfully connected to sqlite!")
except BaseException:
    print("Fail to connect to sqlite database")
    exit(1)

cursor = conn.cursor()

# 400*500000 will create a 8G sqlite file
for i in range(100):  # number of tables
    cursor.execute("create table a%s(a varchar,b varchar,c varchar,d varchar);" % i)
    for j in range(50):  # number of data in each table
        cursor.execute("insert into a%s values ('abaabaca', 'abaabaca', 'abaabaca','abaabacab');" % i)
print("Successfully import.")

all_table = cursor.execute("select count(*) from sqlite_master;").fetchall()
print(all_table[0][0])

conn.commit()
cursor.close()

time_end = time.time()
time_cost = time_end - time_start

print("using time:", round(time_cost, 2), "s")
