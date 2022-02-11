from sql_log_handler import LogDBHandler
import mysql.connector as connection
import logging


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
        try:
            self.mydb_conn = connection.connect(host="localhost", user="root", passwd="Shashu@247", use_pure=True)
            self.cursor = self.mydb_conn.cursor()
        except Exception as e:
            print(e)

    def create_db(self):
        try:
            self.cursor.execute("""create database if not exists {}""".format(self.db_name))
            self.cursor.execute("""use {}""".format(self.db_name))
        except Exception as e:
            print(e)

    def create_table(self):
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
