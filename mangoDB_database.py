import pymongo


class MangoDBConnection:

    def __init__(self, api):
        self.db = None
        self.coll_name = None
        self.t_attr = {}
        self.db_name = None
        self.mydb = None
        self.t_names = []
        self.api = api
        self.cursor = None
        try:
            self.client_cloud = pymongo.MongoClient(api)
        except Exception as e:
            print(e)

    def use_database(self, db_name):
        self.db_name = db_name
        try:
            self.db = self.client_cloud[db_name]
        except Exception as e:
            print(str(e))

    def use_collection(self, coll_name):
        self.coll_name = coll_name
        try:
            self.db = self.db[coll_name]
        except Exception as e:
            print(str(e))

    def insert_one(self, record):
        try:
            self.coll_name.insert_one(record)
        except Exception as e:
            print(e)

    def insert_many(self, list_records):
        try:
            self.coll_name.insert_many(list_records)
        except Exception as e:
            print(e)

    def update(self):
        pass

    def delete(self):
        pass

    def get_data(self):
        pass

    def close_connection(self):
        try:
            self.client_cloud.close()
        except Exception as e:
            print(str(e))
