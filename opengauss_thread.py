import logging
import threading
import time

from connection import OpenGaussConnection


class OpenGaussThread(threading.Thread):

    def __init__(self, opengauss: OpenGaussConnection, sqls):
        super().__init__()
        self.opengauss = opengauss
        self.sqls = sqls

    def run(self) -> None:
        conn = None
        try:
            conn = self.opengauss.getconn()
            # print(self.name)
            # time.sleep(100) # 测试
            """TODO: 连接执行每一个表的创建和插入操作"""
        except Exception as e:
            logging.error(e)
        finally:
            if conn is not None:
                self.opengauss.putconn(conn)
