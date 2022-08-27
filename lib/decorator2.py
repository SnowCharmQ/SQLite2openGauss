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
        sql = convert_datatype(sql)
        sql = not_null(sql)
        return sql

    return wrapper


@original_processed
def createWithoutFK(sql):
    sql = remove_foreign_key(sql)
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
    y = sql.find('[]')
    if y != -1:
        return True
    return False


def array_attribute(sql: str):
    # 返回含有array的属性列的名称
    sql = sql.replace(",", " ")
    attribute = sql.split()
    count = 0
    for i in attribute:
        if i.find('[]'):
            att = attribute[count - 1]
            count += 1
    return att


def autoIncrement(sql: str):
    # 如果系统表中包含 sqlite_sequence （或者有AUTOINCREMENT 关键字）
    # 说明有自增列
    # 那么在create table里面调用此函数
    # 即使用创建的序列与此列关联

    index = sql.find('(')
    table_name = sql[13:index]
    incre_att = ''
    new_sql = ''
    if sql.find("AUTOINCREMENT") != -1 or sql.find("autoincrement") != -1:
        sql = sql[index + 1:]
        for i in sql.split(','):
            print(i)
            if i.find("AUTOINCREMENT") != -1 or i.find("autoincrement") != -1:
                incre_att = i.split()[0]
                if i.find('PRIMARY KEY') != -1 or i.find('primary key') != -1:
                    i = incre_att + " INTEGER " + "PRIMARY KEY"
                else:
                    i = incre_att + " INTEGER "

            new_sql = new_sql + ',' + i

        return 'CREATE TABLE ' + table_name + "(" + new_sql[1:], incre_att
    else:
        return sql, incre_att


@original_processed
def Insert(sql):
    if sql.find('INSERT INTO') != -1:  # 去掉table名称的的双引号
        sql = sql[0:12] + sql[13:]
        index = sql.find("\"")
        sql = sql[:index] + sql[index + 1:]
        sql = insert_array(sql)

    return sql

# createWithoutFK("CREATE TABLE movies(movieid       integer not null primary key," +
#
# ans =("CREATE TABLE book(" +

#              "ID INT PRIMARY KEY     NOT NULL," +

#            "NAME           TEXT    NOT NULL," +

#            "classification        text[]);")
# index= ans.find('(')
# print(ans[13:index])

# string = "   "
# string, att = autoIncrement("CREATE TABLE COMPANY(" +
#                       "NAME           TEXT      NOT NULL," +
#                       "AGE            INT       NOT NULL," +
#                      "ADDRESS        CHAR(50)," +
#                   "SALARY         REAL," +
#                   " ID INTEGER PRIMARY KEY   AUTOINCREMENT);")
# print(string)
# print(att)
# index = string.find('(')
# print(string[13:index])
# string="INSERT INTO 'alt_titles' VALUES(104,1777,'Alexander');"
##print(Insert(string));
# a,b=autoIncrement("CREATE TABLE user("+
# 	"id integer autoincrement primary key,"+
# 	"username character varying,"+
# 	"password character varying);"
#  )
#
# print(a,b)
sql="CREATE TABLE connections(  station_id int    not null    constraint connections_fk    references stations,  connection varchar(100) not null,  constraint connections_pk    primary key (station_id, connection));"
newsql = createWithoutFK(sql)
print(newsql)


def trigger_to_function(trigger_name: str, sql: str):
    function_name = trigger_name + "()"
    sql = sql.upper()
    # dealing with \t and\n
    sql_L = sql.split()
    sql = " ".join(sql_L)

    # locate action
    ll = sql.find("BEGIN") + 6
    rr = sql.find("END") - 2
    action = sql[ll:rr]

    function = "CREATE FUNCTION function_name RETURNS TRIGGER AS $example_table$\n" + "    BEGIN\n" + "        action;\n" + "        RETURN NEW;\n" + "    END;\n" + "$example_table$ LANGUAGE plpgsql;"
    function = function.replace("function_name", function_name)
    function = function.replace("action", action)

    # keywords_needed_to_be_changed
    function = function.replace("DATETIME('NOW')", "CURRENT_TIMESTAMP")
    function = function.replace("json_array", "array")

    print(function)
    return function

def new_trigger(trigger_name: str, sql: str):
    rr = sql.find("BEGIN")
    new_sql = sql[:rr]
    trigger_sql = new_sql + "FOR EACH ROW EXECUTE PROCEDURE " + trigger_name + "()" + ";"
    print(trigger_sql)
    return trigger_sql
