import time
import logging
import sqlite3
from psycopg2 import pool
from threading import Semaphore


class OpenGaussConnectionPool(pool.AbstractConnectionPool):
    def __init__(self, minconn, maxconn, *args, **kwargs):
        self.semaphore = Semaphore(maxconn)
        self.maxconn = maxconn
        self.current = 0
        super().__init__(minconn, maxconn, *args, **kwargs)

    def getconn(self, key=None):
        self.semaphore.acquire()
        self.current += 1
        return self._getconn(key)

    def putconn(self, *args, **kwargs):
        self._putconn(*args, **kwargs)
        self.semaphore.release()
        self.current -= 1

    def closeall(self) -> None:
        super().closeall()
        self.semaphore.release(self.current)


class OpenGaussConnection:
    def __init__(self, opengauss_properties, error_log: logging.Logger, info_log: logging.Logger):
        super().__init__()
        self.pool = None
        self.opengauss_properties = opengauss_properties
        i = 0
        ex = None
        while True:
            if i == 5:
                print("Fail to connect to OpenGauss database")
                raise ex
            try:
                self.pool = OpenGaussConnectionPool(1, 300,
                                                    database=self.opengauss_properties['database.name'],
                                                    user=self.opengauss_properties['database.user'],
                                                    password=self.opengauss_properties[
                                                        'database.password'],
                                                    host=self.opengauss_properties['database.host'],
                                                    port=self.opengauss_properties['database.port'],
                                                    keepalives=1,
                                                    keepalives_idle=30,
                                                    keepalives_interval=10,
                                                    keepalives_count=15)
                info_log.info(
                    "Successfully Log In OpenGauss Database %s As %s" % (
                        self.opengauss_properties['database.name'], self.opengauss_properties['database.user']))
                print("Successfully Log In OpenGauss Database %s As %s" % (
                    self.opengauss_properties['database.name'], self.opengauss_properties['database.user']))
                break
            except Exception as e:
                i += 1
                if i == 1:
                    print("Fail to connect to OpenGauss database. Retry 1 time")
                else:
                    print("Fail to connect to OpenGauss database. Retry %d times" % i)
                error_log.error(e)
                ex = e
                time.sleep(5)

    def getconn(self, key=None):
        return self.pool.getconn(key)

    def putconn(self, *args, **kwargs):
        self.pool.putconn(*args, **kwargs)

    def closeall(self):
        self.pool.closeall()


class SqliteConnection:
    def __init__(self, sqlite_properties, error_log: logging.Logger, info_log: logging.Logger):
        i = 0
        ex = None
        while True:
            if i == 5:
                print("Fail to connect to Sqlite3 database")
                raise ex
            try:
                self.conn_sqlite = sqlite3.connect("sqlite/" + sqlite_properties['database.filename'])
                info_log.info("Successfully Log In Sqlite3 Database %s" % (sqlite_properties['database.filename']))
                print("Successfully Log In Sqlite3 Database %s" % (sqlite_properties['database.filename']))
                break
            except Exception as e:
                i += 1
                if i == 1:
                    print("Fail to connect to Sqlite3 database. Retry 1 time")
                else:
                    print("Fail to connect to Sqlite3 database. Retry %d times" % i)
                error_log.error(e)
                ex = e
                time.sleep(5)

    def getconn(self):
        return self.conn_sqlite
