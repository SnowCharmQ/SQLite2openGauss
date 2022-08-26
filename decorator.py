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

    return fk.replace(',',' ')
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
    sqls=[]
    while True:
        x = sql.find("foreign key")
        if x == -1:
            return sqls
        else:
            sqls.append(try_to_extract(sql, x))
            sql = try_to_remove(sql, x)

    return sqls

def not_null(sql):
    sql=sql.replace(",'',",",' ',")
    sql = sql.replace("('',", "(' ',")
    sql = sql.replace(",'')", ",' ')")
    return sql



def getTableName(sql):
    x = sql.find("CREATE TABLE")
    # print(x)
    y=sql.find("(")
    # print(y)
    # print(sql[x+12:y])
    return sql[x+12:y]
def original_processed(func):
    def wrapper(*args,**kwargs):

        sql=func(*args,**kwargs)
        sql=convert_datetime_to_date(sql)
        sql=not_null(sql)

        # print("##################")
        return sql
    return wrapper





@original_processed
def createWithoutFK(sql):


    sql = remove_foreign_key(sql)



    return sql






def alterFK(sql):

    sqls=extract_foreign_key(sql)
    table_name=getTableName(sql)
    alter_sqls=[]
    for sql in sqls:
        alter_sqls.append("alter table "+table_name+" add "+sql)
    # print(alter_sqls)
    return alter_sqls


@original_processed
def Insert(sql):


    return sql

