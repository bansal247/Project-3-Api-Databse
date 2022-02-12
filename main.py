from mongoDB_database import MongoDBConnection


def try_it():
    mdb = MongoDBConnection(user_name='shashu247', password='shashu247',
                            url='mongodb+srv://shashu247:<password>@cluster0.lwuf5.mongodb.net/myFirstDatabase'
                                '?retryWrites=true&w=majority')
    mdb.connect_mongo_db_client()
    mdb.config('db2', 'c2')
    print(mdb.find_all_records())
    mdb.close_mongo_db_client()


if __name__ == '__main__':
    try_it()
