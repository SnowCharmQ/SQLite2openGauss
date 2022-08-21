




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


def original_processed(func):
    def wrapper(*args,**kwargs):

        sql=func(*args,**kwargs)
        sql=convert_datetime_to_date(sql)

        # print("##################")
        return sql
    return wrapper





@original_processed
def createWithoutFK(sql):


    sql = remove_foreign_key(sql)



    return sql





@original_processed
def alterFK(sql):

    return sql


@original_processed
def Insert(sql):


    return sql

createWithoutFK("CREATE TABLE movies(movieid       integer not null primary key,"+
                    "title         varchar(100) not null"+
                "  constraint 'title length' "+
                                           "    check(length(title)<=100),"+
                                           "    country       char(2) not null"+
                                           "            constraint 'country length'"+
                    "              check(length(country)<=2),"+
                          "   year_released int not null"+
                                  " constraint 'year_released numerical'"+
                                              "      check(year_released+0=year_released),"+
                                         "                 constraint 'runtime numerical'"+
                                                                                                                                                                  "        check(runtime+0=runtime),"+
                                                                                                                                                                  " unique(title, country, year_released),"+
                                                                                                                                                                  "  foreign key(country) references countries(country_code));")

