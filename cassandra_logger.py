from cassandra_log_handler import LogDBHandler
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging
import pandas as pd


class Logger(logging.Logger):
    def __init__(self, client_id, client_secret, path_to_secure_connect_logger_zip, class_name, keyspace_name='logDB',
                 table_name='logs'):
        """

        :param client_id:
        :param client_secret:
        :param path_to_secure_connect_logger_zip:
        :param class_name:
        :param keyspace_name:
        :param table_name:
        """
        self.session = None
        self.cluster = None
        try:
            logging.Logger.__init__(self, name=class_name)
            self.log_h = None
            self.table_name = table_name
            self.keyspace_name = keyspace_name
            self.client_secret = client_secret
            self.path_to_secure_connect_logger_zip = path_to_secure_connect_logger_zip
            self.client_id = client_id
        except Exception as e:
            print(e)

    def create_cassandra_session(self):
        """
        This function creates cassandra connection
        :return:
        """
        try:
            cloud_config = {
                'secure_connect_bundle': self.path_to_secure_connect_logger_zip
            }
            auth_provider = PlainTextAuthProvider(self.client_id, self.client_secret)
            self.cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = self.cluster.connect()
        except Exception as e:
            print(e)

    def connect_keyspace(self):
        """
        This function connect keyspace
        :return:
        """
        try:
            self.session.execute("""use {}""".format(self.keyspace_name))
        except Exception as e:
            print(e)

    def create_table(self):
        """
        This function creates table
        :return:
        """
        try:
            self.session.execute("create table if not exists {} (id bigint primary key , log_level int, "
                                 "level_name text,created_at timestamp,file_name text,"
                                 "class_name text,function_name text,line_no bigint,"
                                 "log_msg text,thread_no bigint,thread_name text,"
                                 "error_text text)".format(self.table_name))
        except Exception as e:
            print(e)

    def initialize(self):
        """
        This function create connection then connects keyspace then creates table then adds cassandra_handler
        :return:
        """
        try:
            self.create_cassandra_session()
            self.connect_keyspace()
            self.create_table()
            self.log_h = LogDBHandler(self.cluster, self.session, self.table_name)
            self.log_h.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s', "%Y-%m-%d %H:%M:%S")
            self.log_h.setFormatter(formatter)
            self.addHandler(self.log_h)
        except Exception as e:
            print(e)

    def get_csv_logfile(self, filename='logs'):
        """
        get csv log file
        :param filename: default logs
        :return:
        """
        try:
            db = pd.DataFrame(self.session.execute("select * from {}".format(filename)).all())
            db.to_csv(filename + ".csv")
        except Exception as e:
            print(e)

    def get_text_logfile(self, filename='logs'):
        """
        get .log logfile
        :param filename: default logs
        :return:
        """
        try:
            db = pd.DataFrame(self.session.execute("select * from {}".format(filename)).all())
            db.to_csv(filename + ".log")
        except Exception as e:
            print(e)

    def get_all_logs(self, filename='logs'):
        """

        :return:
        """
        try:
            return self.session.execute("select * from {}".format(filename)).all()
        except Exception as e:
            print(e)

    def close_session(self):
        """
        To close the session
        :return:
        """
        try:
            self.session.shutdown()
        except Exception as e:
            print(e)
