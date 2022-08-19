import time
import logging
import threading
import psycopg2


class OpenGaussThread(threading.Thread):

    def __init__(self, opengauss_properties):
        super().__init__()
        self.conn_opengauss = None
        self.opengauss_properties = opengauss_properties

    def run(self) -> None:
        i = 0
        ex = None
        while True:
            if i == 5:
                print("Fail to connect to OpenGauss database")
                raise ex
            try:
                self.conn_opengauss = psycopg2.connect(database=self.opengauss_properties['database.name'],
                                                       user=self.opengauss_properties['database.user'],
                                                       password=self.opengauss_properties['database.password'],
                                                       host=self.opengauss_properties['database.host'],
                                                       port=self.opengauss_properties['database.port'])
                logging.info(
                    "Successfully Log In OpenGauss Database %s As %s" % (
                        self.opengauss_properties['database.name'], self.opengauss_properties['database.user']))
                break
            except Exception as e:
                i += 1
                if i == 1:
                    print("Fail to connect to OpenGauss database. Retry 1 time")
                else:
                    print("Fail to connect to OpenGauss database. Retry %d times" % i)
                logging.error(e)
                ex = e
                time.sleep(5)
