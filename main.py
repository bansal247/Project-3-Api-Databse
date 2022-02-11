from sql_database import SqlConnection


def try_it():
    mydb = SqlConnection(host="localhost", user="root", passwd="Shashu@247")
    mydb.close_connection()


if __name__ == '__main__':
    try_it()
