import logging
import threading

from lib import decorator
from lib.connection import OpenGaussConnection


class OpenGaussThread(threading.Thread):

    def __init__(self, opengauss: OpenGaussConnection, sqls, dbschema, error_log: logging.Logger):
        super().__init__()
        self.opengauss = opengauss
        self.sqls = sqls
        self.dbschema = dbschema
        self.error_log = error_log

    def run(self) -> None:
        conn = None
        try:
            conn = self.opengauss.getconn()
            cursor_opengauss = conn.cursor()
            cursor_opengauss.execute("set search_path to %s;" % self.dbschema)
            for sql in self.sqls:
                if sql.upper().startswith("CREATE"):
                    sql = decorator.create_without_fk(sql)
                    cursor_opengauss.execute(sql)
                elif sql.upper().startswith("BEGIN TRANSACTION;") or sql.upper().startswith("COMMIT;"):
                    continue
                else:
                    sql = decorator.insert(sql)
                    cursor_opengauss.execute(sql)
            conn.commit()
        except Exception as e:
            self.error_log.error(e)
        finally:
            if conn is not None:
                self.opengauss.putconn(conn)


class OpenGaussLogThread(OpenGaussThread):

    def __init__(self, opengauss: OpenGaussConnection, sqls, dbschema, error_log: logging.Logger,
                 sqls_log: logging.Logger):
        super().__init__(opengauss, sqls, dbschema, error_log)
        self.sqls_log = sqls_log

    def run(self) -> None:
        conn = None
        try:
            conn = self.opengauss.getconn()
            cursor_opengauss = conn.cursor()
            cursor_opengauss.execute("set search_path to %s;" % self.dbschema)
            for sql in self.sqls:
                if sql.upper().startswith("CREATE"):
                    continue
                elif sql.upper().startswith("BEGIN TRANSACTION;") or sql.upper().startswith("COMMIT;"):
                    continue
                else:
                    sql = decorator.insert(sql)
                    cursor_opengauss.execute(sql)
                self.sqls_log.info(sql.replace("\n", ""))
            conn.commit()
        except Exception as e:
            self.error_log.error(e)
        finally:
            if conn is not None:
                self.opengauss.putconn(conn)
