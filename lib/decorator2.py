import re


def find_n_sub_str(src: str, sub: str, n: int, start: int):
    index = src.find(sub, start)
    if index != -1 and n > 0:
        return find_n_sub_str(src, sub, n - 1, index + 1)
    return index


def find_n_sub_str_re(src: str, pattern: re.Pattern, n: int, start: int):
    search = pattern.search(src, start)
    index = search.start()
    sub = search.group()
    if index != -1 and n > 0:
        return find_n_sub_str_re(src, pattern, n - 1, index + 1)
    return index, sub


def check_integrity(src: str, sub: str, index: int):
    flag = True
    if index > 0:
        flag = (src[index - 1] == ' ')
        if not flag:
            return flag
    if index + len(sub) < len(src):
        flag = (src[index + len(sub)] == ' ')
    return flag


def check_is_column_name(src: str, index: int):
    index -= 1
    if src[index] != ' ':
        return False
    else:
        return True


def convert_to_not_null(sql: str):
    cnt = sql.count("''")
    flag = True
    for n in range(cnt):
        index = find_n_sub_str(sql, "''", n, 0)
        if index > 0:
            if sql[index - 1] != '(' and sql[index - 1] != ',' and sql[index - 1] != ' ':
                flag = False
        if index + 2 < len(sql):
            if sql[index + 2] != ')' and sql[index + 2] != ',' and sql[index + 2] != ' ':
                flag = False
        if flag:
            sql = sql[0:index] + "' '" + sql[(index + 2):]
        flag = True
    return sql


def convert_double_quote(sql: str):
    oldsql = None
    if sql.startswith("CREATE TABLE"):
        index = sql.upper().find("(")
        oldsql = sql[0:(index + 1)]
        sql = sql[(index + 1):]
    if sql.strip().startswith("'"):
        start = sql.find("'")
        for i in range(start + 1, len(sql)):
            if sql[i] == "'":
                if oldsql is not None:
                    return oldsql + '"' + sql[(start + 1):i] + '"' + sql[i + 1:]
                else:
                    return '"' + sql[(start + 1):i] + '"' + sql[i + 1:]
    else:
        if oldsql is not None:
            return oldsql + sql
        else:
            return sql


def convert_varchar(sql: str):
    pattern = re.compile('VARCHAR[(]\d+[)]')
    cnt = len(pattern.findall(sql.upper()))
    for n in range(cnt):
        index, sub = find_n_sub_str_re(sql.upper(), pattern, n, 0)
        if check_integrity(sql.upper(), sub, index):
            num = int(sub[8:-1])
            num *= 3
            num = str(num)
            result = "VARCHAR(%s)" % num
            sql = sql[0:index] + result + sql[(index + len(sub)):]
    return sql


def try_to_convert(oldstr: str, newstr: str, sql: str):
    cnt = sql.upper().count(oldstr.upper())
    if cnt > 0:
        for n in range(cnt):
            index = find_n_sub_str(sql.upper(), oldstr.upper(), n, 0)
            if check_integrity(sql.upper(), oldstr.upper(), index) \
                    and check_is_column_name(sql.upper(), index):
                return sql[0:index] + newstr + sql[(index + len(oldstr)):]
    return sql


def try_to_remove_fk(sql: str):
    sql = convert_double_quote(sql)
    if sql.upper().find("FOREIGN KEY") != -1:
        if sql.endswith("));") or sql.endswith(");"):
            return ");"
        else:
            return ""
    cnt = sql.upper().count("REFERENCES")
    if cnt > 0:
        for n in range(cnt):
            index1 = find_n_sub_str(sql.upper(), "REFERENCES", n, 0)
            if check_integrity(sql.upper(), "REFERENCES", index1):
                index2 = sql.upper().find("CONSTRAINT")
                if index2 != -1:
                    return sql[0:index2]
                else:
                    return sql[0:index1]
    return sql


def remove_foreign_key(sql: str):
    ss = sql.split(',')
    ss = map(lambda x:
             try_to_remove_fk(x.replace('\n', '').replace('\r', '').replace('   ', ' ').strip()), ss)
    sss = list(filter(lambda x: (x != ''), ss))
    sql = ",".join(sss)
    sql = sql.replace(",);", ");").replace("(,", "(").replace(",,", ",")
    return sql


def extract_foreign_key(sql: str):
    sqls = []
    ss = sql.split(',')
    for sql in ss:
        sql = sql.replace('\n', '').replace('\r', '').replace('   ', ' ').strip()
        index = sql.upper().find("FOREIGN KEY")
        if index != -1:
            sql = sql[index:]
            while sql.endswith(")") or sql.endswith(";") or sql.endswith(","):
                sql = sql[0:-1]
            index = sql.upper().find("REFERENCES")
            if sql.find("(", index) != -1 and sql.find(")", index) == -1:
                sql = sql + ")"
            sqls.append(sql)
            continue
        cnt = sql.upper().count("REFERENCES")
        if cnt > 0:
            for n in range(cnt):
                index = find_n_sub_str(sql.upper(), "REFERENCES", n, 0)
                fk = sql[index:]
                if check_integrity(sql.upper(), "REFERENCES", index):
                    column_name = None
                    if sql.upper().startswith("CREATE TABLE"):
                        index = sql.upper().find("(")
                        sql = sql[(index + 1):]
                    if sql.strip().upper().startswith('"'):
                        start = sql.find('"')
                        for i in range(start + 1, len(sql)):
                            if sql[i] == '"':
                                column_name = sql[start:i + 1]
                                break
                    elif sql.upper().startswith("'"):
                        start = sql.find("'")
                        for i in range(start + 1, len(sql)):
                            if sql[i] == "'":
                                column_name = '"' + sql[(start + 1):i] + '"'
                                break
                    else:
                        sql = sql.strip()
                        for i in range(len(sql)):
                            if sql[i] == ' ':
                                column_name = sql[0:i]
                                break
                    result = "foreign key (%s) %s" % (column_name, fk)
                    sqls.append(result)
                    break
    return sqls


def convert_datatype(sql: str):
    sql = convert_varchar(sql)
    sql = try_to_convert("datetime", "timestamp without time zone", sql)
    sql = try_to_convert("real", "double precision", sql)
    sql = try_to_convert("nvarchar", "nvarchar2", sql)
    sql = try_to_convert("varying character", "character varying", sql)
    sql = try_to_convert("graphic", "nchar", sql)
    sql = try_to_convert("year", "integer", sql)
    sql = try_to_convert("line", "path", sql)
    return sql


def get_table_name(sql):
    x = sql.find("CREATE TABLE")
    y = sql.find("(")
    return sql[x + 12:y]


def createWithoutFK(sql):
    sql = remove_foreign_key(sql)
    sql = convert_datatype(sql)
    sql = convert_to_not_null(sql)
    return sql


def alterFK(sql):
    sqls = extract_foreign_key(sql)
    table_name = get_table_name(sql)
    alter_sqls = []
    for sql in sqls:
        alter_sqls.append("alter table " + table_name + " add " + sql)
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


def Insert(sql):
    if sql.find('INSERT INTO') != -1:  # 去掉table名称的的双引号
        sql = sql[0:12] + sql[13:]
        index = sql.find("\"")
        sql = sql[:index] + sql[index + 1:]
        sql = insert_array(sql)
        sql = convert_to_not_null(sql)
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
# sql = "CREATE TABLE connections(  station_id int    not null    constraint connections_fk    references stations,  connection varchar(100) not null,  constraint connections_pk    primary key (station_id, connection));"
# newsql = createWithoutFK(sql)
# print(newsql)


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
