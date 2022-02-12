from mongoDB_log_handler import LogDBHandler
import pymongo
import logging
import pandas as pd


class Logger(logging.Logger):
    def __init__(self, user_name, password, url, class_name, db_name='logDB', collection_name='logs',
                 console_output=False):
        """
        This is Mongo DB cloud based logging system
        :param user_name:
        :param password:
        :param url: string, url for application
        :param class_name: use self.__class__.__name__
        :param db_name: default logDB
        :param collection_name: default logs
        :param console_output: True or False if true logs will display on console
        """

        try:
            logging.Logger.__init__(self, name=class_name)
            self.log_h = None
            self.stream_h = None
            self.database = None
            self.collection = None
            self.console_output = console_output
            self.collection_name = collection_name
            self.db_name = db_name
            self.password = password
            self.user_name = user_name
            self.client = None
            left = url.find('://')
            right = url.find('<password>')
            word = url[left + 3:right - 1]
            url = url.replace(word, user_name)
            url = url.replace('<password>', password)
            self.url = url.replace('myFirstDatabase', db_name)
        except Exception as e:
            print(e)

    def connect_mongo_db_client(self):
        """
        This function creates mongo db client
        :return:
        """
        try:
            self.client = pymongo.MongoClient(self.url)
        except Exception as e:
            print(e)

    def connect_db(self):
        """
        This function creates or connects database
        :return:
        """
        try:
            self.database = self.client[self.db_name]
        except Exception as e:
            print(e)

    def create_collection(self):
        """
        This function creates collection
        :return:
        """
        try:
            self.collection = self.database[self.collection_name]
        except Exception as e:
            print(e)

    def initialize(self):
        """
        This function create client connection then creates database then creates collection
        then adds mangoDB_handler
        :return:
        """
        try:
            self.connect_mongo_db_client()
            self.connect_db()
            self.create_collection()
            self.log_h = LogDBHandler(self.client, self.database, self.collection)
            self.log_h.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s', "%Y-%m-%d %H:%M:%S")
            self.log_h.setFormatter(formatter)
            if self.console_output:
                formatter_console = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(lineno)s - %('
                                                      'levelname)s - %(message)s', "%Y-%m-%d %H:%M:%S")
                self.stream_h = logging.StreamHandler()
                self.stream_h.setFormatter(formatter_console)
                self.addHandler(self.stream_h)
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
            db = pd.DataFrame(list(self.collection.find()))
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
            db = pd.DataFrame(list(self.collection.find()))
            db.to_csv(filename + ".log")
        except Exception as e:
            print(e)

    def close_client(self):
        """
        To close the client
        :return:
        """
        try:
            self.client.close()
        except Exception as e:
            print(e)
