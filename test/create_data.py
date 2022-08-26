import sqlite3
import time

# 2000*50 will create a 8M file

time_start = time.time()

try:
    conn = sqlite3.connect('E:\\lessons\\lessons\\2022_summer\\openGauss\\Test.sqlite')           # position of sqlite file
    print("Successfully connected to sqlite!")
except BaseException:
    print("Fail to connect to sqlite database")
    exit(1)

cursor = conn.cursor()

for i in range(200):                                                                                    # number of tables
    cursor.execute("create table a%s(id integer primary key autoincrement,test_real real,test_nvarchar nvarchar,"
                   "test_varing_character varing character(16),test_datetime datetime,test_graphic graphic(16),"
                   "test_year year);" % i)
    for j in range(50):                                                                                 # number of data in each table
        cursor.execute("insert into a%s(test_real,test_nvarchar,test_varing_character,test_datetime,test_graphic,"
                       "test_year) values (3.1415926,'测试','test',2022-08-30,'测试', 2022);" % i)
print("Successfully import.")

all_table = cursor.execute("select count(*) from sqlite_master;").fetchall()
print(all_table[0][0])

conn.commit()
cursor.close()

time_end = time.time()
time_cost = time_end-time_start

print("using time:",round(time_cost,2),"s")
