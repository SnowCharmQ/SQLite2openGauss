def convert_datatype(sql: str):
    sql = sql.replace("datetime", "timestamp without time zone")
    sql = sql.replace("real", "double precision")
    sql = sql.replace("nvarchar", "nvarchar2")
    sql = sql.replace("varying character", "character varying")
    sql = sql.replace("graphic", "nchar")
    sql = sql.replace("year", "integer")
    sql = sql.replace("line", "path")

    return sql


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


def try_to_extract(sql: str, x: int):
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

    return fk.replace(',', ' ')


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


def extract_foreign_key(sql: str):
    ss = sql.split('\n')
    map(lambda x: x.strip(), ss)
    sql = "".join(ss)
    sqls = []
    while True:
        x = sql.find("foreign key")
        if x == -1:
            return sqls
        else:
            sqls.append(try_to_extract(sql, x))
            sql = try_to_remove(sql, x)

    return sqls


def not_null(sql):
    sql = sql.replace(",'',", ",' ',")
    sql = sql.replace("('',", "(' ',")
    sql = sql.replace(",'')", ",' ')")
    return sql


def getTableName(sql):
    x = sql.find("CREATE TABLE")
    # print(x)
    y = sql.find("(")
    # print(y)
    # print(sql[x+12:y])
    return sql[x + 12:y]


def original_processed(func):
    def wrapper(*args, **kwargs):
        sql = func(*args, **kwargs)



        # print("##################")
        return sql

    return wrapper


@original_processed
def createWithoutFK(sql):
    sql = remove_foreign_key(sql)
    sql = convert_datatype(sql)
    sql = not_null(sql)

    return sql


def alterFK(sql):
    sqls = extract_foreign_key(sql)
    table_name = getTableName(sql)
    alter_sqls = []
    for sql in sqls:
        alter_sqls.append("alter table " + table_name + " add " + sql)
    # print(alter_sqls)
    return alter_sqls


def insert_array(sql: str):
    x = sql.find(",'[")
    y = sql.find("]")
    if x != -1 and y != -1:
        sql = sql[:x + 1] + 'array' + sql[x + 2:y + 1] + sql[y + 2:]

    return sql


def haveArrayType(sql: str):
    # 判断create table 里面的数据类型是否有数组
    # 如果有，insert操作时，执行insert_array
    x = sql.find("CREATE TABLE")
    if x != -1:
        y = sql.find('[]')
        if y != -1:
            return True
    return False

def autoIncrement(sql: str):
    # 如果系统表中包含 sqlite_sequence （或者有AUTOINCREMENT 关键字）
    # 说明有自增列
    # 那么在create table里面调用此函数
    # 即使用序列整型与此列关联
    sql = sql.replace("AUTOINCREMENT", "SERIAL")
    return sql

def insert_autoIncrement(sql:str):
    # insert时调用此函数, 将相应自增列插入值 : default
    # 具体插入哪一列可查看 sqlite_sequenc 表
    return sql



@original_processed
def Insert(sql):
    sql = sql.replace("\"", "'")
    sql = insert_array(sql)
    return sql


createWithoutFK("CREATE TABLE movies(movieid       integer not null primary key," +
                "title         varchar(100) not null" +
                "  constraint 'title length' " +
                "    check(length(title)<=100)," +
                "    country       char(2) not null" +
                "            constraint 'country length'" +
                "              check(length(country)<=2)," +
                "   year_released int not null" +
                " constraint 'year_released numerical'" +
                "      check(year_released+0=year_released)," +
                "                 constraint 'runtime numerical'" +
                "        check(runtime+0=runtime)," +
                " unique(title, country, year_released)," +
                "  foreign key(country) references countries(country_code));")
