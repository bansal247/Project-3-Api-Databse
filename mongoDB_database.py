import pymongo
from mongoDB_logger import Logger


class MongoDBConnection:

    def __init__(self, user_name, password, url):
        try:
            self.logger = Logger(user_name, password, "mongodb+srv://shashu247:<password>@cluster0.5yvca.mongodb.net"
                                                      "/myFirstDatabase?retryWrites=true&w=majority",
                                 self.__class__.__name__)
            self.logger.initialize()
        except Exception as e:
            raise Exception("Something went wring with logger SEE CONSOLE  " + str(e))
        try:
            self.collection = None
            self.collection_name = None
            self.db_name = None
            self.database = None
            self.client = None
            self.user_name = user_name
            self.password = password
            self.unchanged_url = url
            left = url.find('://')
            right = url.find('<password>')
            word = url[left + 3:right - 1]
            url = url.replace(word, user_name)
            url = url.replace('<password>', password)
            self.url = url
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
            self.logger.error(e)

    def close_mongo_db_client(self):
        """
        To close the client
        :return:
        """
        try:
            self.client.close()
        except Exception as e:
            self.logger.error(e)

    def is_database_present(self, db_name):
        """
        This function  checks if the given database is present or not
        :param db_name:
        :return: bool True or False
        """
        try:
            if db_name in self.client.list_database_names():
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(e)

    def create_database(self, db_name):
        """
        This function create the database
        :param db_name: database name
        :return:
        """

        try:
            self.database = self.client[db_name]
            self.db_name = db_name
        except Exception as e:
            self.logger.error(e)
        else:
            self.logger.info("{} database created".format(db_name))

    def drop_database(self, db_name=None):
        """
        This function drop the database
        :param db_name: name of database
        :return:
        """
        try:
            if db_name is None:
                db_name = self.db_name
            if self.is_database_present(db_name):
                self.client.drop_database(db_name)
        except Exception as e:
            self.logger.error(e)
        else:
            self.logger.info("""{} database is dropped""".format(db_name))

    def get_database(self, db_name=None):
        """
        This function return database
        :return:
        """
        try:
            if db_name is None:
                db_name = self.db_name
            return self.client[db_name]
        except Exception as e:
            self.logger.error(e)

    def get_all_databases(self):
        """
        This function return all the available databases
        :return:
        """
        try:
            return self.client.list_database_names()
        except Exception as e:
            self.logger.error(e)

    def config(self, db_name, collection_name):
        """
        It sets the database and table for whole object so that db_name and table_name is not required
        in other functions
        :param db_name:
        :param collection_name:
        :return:
        """
        try:
            self.set_database(db_name)
            self.set_collection(collection_name)
        except Exception as e:
            self.logger.error(e)

    def set_database(self, db_name):
        """
        This Function use the database and set as default database
        :param db_name: database name
        :return:
        """

        try:
            self.db_name = db_name
            self.database = self.client[db_name]
            self.logger.info("Database set to {}".format(db_name))
        except Exception as e:
            self.logger.error(e)

    def set_collection(self, collection_name):
        """
        This Function set table as default table
        :param collection_name: collection_name
        :return:
        """
        try:
            self.collection_name = collection_name
            self.collection = self.database[collection_name]
            self.logger.info("collection_name set to {}".format(collection_name))
        except Exception as e:
            self.logger.error(e)

    def get_collection(self, db_name=None, collection_name=None):
        """

        :param collection_name:
        :param db_name:
        :return:
        """
        try:
            if collection_name is None:
                collection_name = self.collection_name
            if db_name is None:
                db_name = self.db_name
            if self.is_database_present(db_name):
                return self.get_database(db_name)[collection_name]
            else:
                return "No such collection present"
        except Exception as e:
            self.logger.error(e)

    def is_collection_present(self, db_name=None, collection_name=None):
        """

        :param collection_name:
        :param db_name:
        :return:
        """
        try:
            if collection_name is None:
                collection_name = self.collection_name
            if db_name is None:
                db_name = self.db_name
            if self.is_database_present(db_name):
                if collection_name in self.get_database(db_name).list_collection_names():
                    return True
                else:
                    return False
        except Exception as e:
            self.logger.error(e)

    def create_collection(self, db_name=None, collection_name=None):
        """
        :param db_name:
        :param collection_name: a string
        :return: None
        """
        try:
            if db_name is None:
                db_name = self.db_name
            if not self.is_collection_present(db_name, collection_name):
                self.collection = self.get_database(db_name)[collection_name]
                self.collection_name = collection_name
        except Exception as e:
            self.logger.error(e)
        else:
            self.logger.info("Created collection {}".format(collection_name))

    def drop_collection(self, db_name=None, collection_name=None):
        """
        Drop the given collection in given database
        :param db_name:
        :param collection_name:
        :return:
        """
        try:
            if db_name is None:
                db_name = self.db_name
            if collection_name is None:
                collection_name = self.collection_name
            if self.is_collection_present(db_name, collection_name):
                self.get_collection(db_name, collection_name).drop()
        except Exception as e:
            self.logger.error(e)
        else:
            self.logger.info("dropped collection {}".format(collection_name))

    def insert_record(self, record, db_name=None, collection_name=None):
        """
        Insert one record in the given collection and database
        :param db_name:
        :param record: A dictionary
        :param collection_name:
        :return:
        """
        try:
            if collection_name is None:
                collection_name = self.collection_name
            self.get_collection(db_name, collection_name).insert_one(record)
        except Exception as e:
            self.logger.error(e)
        else:
            self.logger.info("""{} record added in collection {}""".format(record, collection_name))

    def insert_records(self, records, db_name=None, collection_name=None):
        """
        This function insert records in given collection
        :param collection_name: collection_name
        :param db_name:
        :param records: list of dictionary
        :return: None
        """
        try:
            if collection_name is None:
                collection_name = self.collection_name
            if db_name is None:
                db_name = self.db_name
            if self.is_collection_present(db_name, collection_name):
                collection = self.get_collection(db_name, collection_name)
                collection.insert_many(records)
        except Exception as e:
            self.logger.error(e)
        else:
            self.logger.info("""records added in collection {}""".format(collection_name))

    def find_first_record(self, query, db_name=None, collection_name=None):
        """

        :param query:
        :param db_name:
        :param collection_name:
        :return:
        """
        try:
            if collection_name is None:
                collection_name = self.collection_name
            if db_name is None:
                db_name = self.db_name
            if self.is_collection_present(db_name, collection_name):
                collection = self.get_collection(db_name, collection_name)
                first_record = collection.find_one(query)
                return first_record
        except Exception as e:
            self.logger.error(e)

    def find_all_records(self, db_name=None, collection_name=None):
        """
        :param db_name:
        :param collection_name:
        :return:
        """
        try:
            if collection_name is None:
                collection_name = self.collection_name
            if db_name is None:
                db_name = self.db_name
            if self.is_collection_present(db_name, collection_name):
                collection = self.get_collection(db_name, collection_name)
                records = list(collection.find())
                return records
        except Exception as e:
            self.logger.error(e)

    def find_record_on_query(self, query, db_name=None, collection_name=None):
        """

        :param query:
        :param db_name:
        :param collection_name:
        :return:
        """
        try:
            if collection_name is None:
                collection_name = self.collection_name
            if db_name is None:
                db_name = self.db_name
            if self.is_collection_present(db_name, collection_name):
                collection = self.get_collection(db_name, collection_name)
                record = collection.find(query)
                return record
        except Exception as e:
            self.logger.error(e)

    def update_one_record(self, r_filter, r_update, db_name=None, collection_name=None):
        """

        :param r_update:
        :param r_filter:
        :param db_name:
        :param collection_name:
        :return:
        """
        try:
            if collection_name is None:
                collection_name = self.collection_name
            if db_name is None:
                db_name = self.db_name
            if self.is_collection_present(db_name, collection_name):
                collection = self.get_collection(db_name, collection_name)
                record = collection.update_one(r_filter, r_update)
                self.logger.info("{} updated to {}".format(r_filter, r_update))
                return record
        except Exception as e:
            self.logger.error(e)

    def update_multiple_records(self, r_filter, r_update, db_name=None, collection_name=None):
        """

        :param r_update:
        :param r_filter:
        :param db_name:
        :param collection_name:
        :return:
        """
        try:
            if collection_name is None:
                collection_name = self.collection_name
            if db_name is None:
                db_name = self.db_name
            if self.is_collection_present(db_name, collection_name):
                collection = self.get_collection(db_name, collection_name)
                record = collection.update_many(r_filter, r_update)
                self.logger.info("{} updated to {}".format(r_filter, r_update))
                return record
        except Exception as e:
            self.logger.error(e)

    def delete_record(self, query, db_name=None, collection_name=None):
        """
        This function delete record
        :param query:
        :param db_name:
        :param collection_name:
        :return:
        """
        try:
            if collection_name is None:
                collection_name = self.collection_name
            if db_name is None:
                db_name = self.db_name
            if self.is_collection_present(db_name, collection_name):
                collection = self.get_collection(db_name, collection_name)
                collection.delete_one(query)
        except Exception as e:
            self.logger.error(e)
        else:
            self.logger.info("{} deleted".format(query))

    def delete_records(self, query, db_name=None, collection_name=None):
        """
        This function delete records
        :param query:
        :param db_name:
        :param collection_name:
        :return:
        """
        try:
            if collection_name is None:
                collection_name = self.collection_name
            if db_name is None:
                db_name = self.db_name
            if self.is_collection_present(db_name, collection_name):
                collection = self.get_collection(db_name, collection_name)
                collection.delete_many(query)
        except Exception as e:
            self.logger.error(e)
        else:
            self.logger.info("{} deleted".format(query))
