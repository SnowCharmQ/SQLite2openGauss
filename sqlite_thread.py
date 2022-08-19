import time
import logging
import threading
import sqlite3


class SqliteThread(threading.Thread):
    def __init__(self, sqlite_properties):
        super().__init__()
        self.conn_sqlite = None
        self.sqlite_properties = sqlite_properties

    def run(self) -> None:
        i = 0
        ex = None
        while True:
            if i == 5:
                print("Fail to connect to Sqlite3 database")
                raise ex
            try:
                self.conn_sqlite = sqlite3.connect(self.sqlite_properties['database.filename'])
                logging.info("Successfully Log In Sqlite3 Database %s" % (self.sqlite_properties['database.filename']))
                break
            except Exception as e:
                i += 1
                if i == 1:
                    print("Fail to connect to Sqlite3 database. Retry 1 time")
                else:
                    print("Fail to connect to Sqlite3 database. Retry %d times" % i)
                logging.error(e)
                ex = e
                time.sleep(5)
