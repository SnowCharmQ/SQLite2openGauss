import logging
import threading
import decorator
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
            cursor_opengauss=conn.cursor()
            # print(self.name)
            # time.sleep(100) # 测试
            """
            TODO: 连接执行每一个表的创建和插入操作
            我的思路：
            使用decorator设计模式
            传入的整段sql语句先对顶上的create语句进行分析（可以做的优化：
            删除所有外键、主键等（因为可以保证数据的正确性）->可以考虑使用一个类变量存储，在主方法里回收，
            对不兼容的数据格式进行修改
            insert语句优化执行（整段执行等）
            ）
            """

            for sql in self.sqls:
                if sql.find("CREATE") != -1:
                    sql=decorator.createWithoutFK(sql)
                    cursor_opengauss.execute(sql)
                    print(sql)
                    print("@@@@@@@@@@@@@@@@@@@@@@")
                else:
                    sql=decorator.Insert(sql)
                    cursor_opengauss.execute(sql)
                    # print(sql)
                    # print("----")
            conn.commit()
                # print(sql)



        except Exception as e:
            logging.error(e)
        finally:
            if conn is not None:
                self.opengauss.putconn(conn)

