from sql_log_handler import LogDBHandler
import mysql.connector as connection
import logging
import pandas as pd


class Logger(logging.Logger):
    def __init__(self, host, user, passwd, class_name, db_name='logDB', table_name='logs'):
        """
        This is sql based logging system
        :param class_name: add self.__class__.__name__
        :param host: host
        :param user: user name
        :param passwd: password
        :param db_name: database name to create default:logDB
        :param table_name: table name to create default:logs
        """
        try:
            logging.Logger.__init__(self, name=class_name)
            self.log_h = None
            self.cursor = None
            self.table_name = table_name
            self.db_name = db_name
            self.passwd = passwd
            self.user = user
            self.host = host
            self.mydb_conn = None
        except Exception as e:
            print(e)

    def create_sql_connection(self):
        """
        This function creates sql connection
        :return:
        """
        try:
            self.mydb_conn = connection.connect(host="localhost", user="root", passwd="Shashu@247", use_pure=True)
            self.cursor = self.mydb_conn.cursor()
        except Exception as e:
            print(e)

    def create_db(self):
        """
        This function creates database
        :return:
        """
        try:
            self.cursor.execute("""create database if not exists {}""".format(self.db_name))
            self.cursor.execute("""use {}""".format(self.db_name))
        except Exception as e:
            print(e)

    def create_table(self):
        """
        This function creates table
        :return:
        """
        try:
            self.cursor.execute(
                "create table if not exists {} (id bigint auto_increment primary key , log_level int null , "
                "level_name varchar(32) null,created_at datetime not null,file_name varchar(100) null,"
                "class_name varchar(100) null,function_name varchar(100) null,line_no bigint null,"
                "log_msg varchar(2048) not null,thread_no bigint null,thread_name varchar(100) null,"
                "error_text varchar(2048) null)".format(
                    self.table_name))
        except Exception as e:
            print(e)

    def initialize(self):
        """
        This function create connection then creates database then creates table then adds sql_handler
        :return:
        """
        try:
            self.create_sql_connection()
            self.create_db()
            self.create_table()
            self.log_h = LogDBHandler(self.mydb_conn, self.cursor, self.table_name)
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
            db = pd.read_sql("""select * from {}""".format(self.table_name), self.mydb_conn)
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
            db = pd.read_sql("select * from {}".format(self.table_name), self.mydb_conn)
            db.to_csv(filename + ".log")
        except Exception as e:
            print(e)

    def close_db(self):
        """
        To close the database
        :return:
        """
        try:
            self.mydb_conn.close()
        except Exception as e:
            print(e)
