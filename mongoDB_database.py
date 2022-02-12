import mysql.connector as connection
from sql_logger import Logger


class SqlConnection:

    def __init__(self, host, user, passwd):
        """
        This class is used to connect mysql

        :param host:
        :param user:
        :param passwd:
        """
        try:
            self.logger = Logger(host, user, passwd, self.__class__.__name__)
            self.logger.initialize()
        except Exception as e:
            raise Exception("Something went wring with logger SEE CONSOLE  " + str(e))

        try:
            self.table_name = None
            self.t_attr = {}
            self.db_name = None
            self.mydb = None
            self.t_names = []
            self.host = host
            self.user = user
            self.passwd = passwd
            self.cursor = None
            self.mydb = connection.connect(host=self.host, user=self.user, passwd=self.passwd, use_pure=True)
            self.cursor = self.mydb.cursor()
        except Exception as e:
            self.logger.error(e)

    def get_sql_connection(self):
        """
        This function return the sql connection
        :return:
        """
        try:
            return self.mydb
        except Exception as e:
            self.logger.error(e)

    def get_sql_cursor(self):
        """
        This function returns the sql cursor
        :return:
        """
        try:
            return self.cursor
        except Exception as e:
            self.logger.error(e)

    def is_database_present(self, db_name):
        """
        This function  checks if the given database is present or not
        :param db_name:
        :return: bool True or False
        """
        try:
            self.cursor.execute("SHOW DATABASES")
            db_list = self.cursor.fetchall()
            for i in db_list:
                if db_name == i[0]:
                    return True
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
            query = """create database if not exists {}""".format(db_name)
            self.cursor.execute(query)
            self.cursor.execute("use {}".format(db_name))
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            self.logger.error(e)
        else:
            self.mydb.commit()
            self.logger.info("""{} database is created""".format(db_name))

    def drop_database(self, db_name=None):
        """
        This function drop the database
        :param db_name: name of database
        :return:
        """
        try:
            if db_name is None:
                db_name = self.db_name
            query = "drop database if exists {}".format(db_name)
            self.cursor.execute(query)
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            self.logger.error(e)
        else:
            self.mydb.commit()
            self.logger.info("""{} database is dropped""".format(db_name))

    def get_databases(self):
        """
        This function return all the available databases
        :return:
        """
        try:
            self.cursor.execute("SHOW DATABASES")
            lst = self.cursor.fetchall()
            db_list = []
            for i in lst:
                db_list.append(i[0])
            self.logger.info("databases returned")
            return db_list
        except Exception as e:
            self.logger.error(e)

    def config(self, table_name, db_name):
        """
        It sets the database and table for whole object so that db_name and table_name is not required
        in other functions
        :param db_name:
        :param table_name:
        :return:
        """
        try:
            self.set_database(db_name)
            self.set_table(table_name)
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
            self.cursor.execute("use {}".format(db_name))
            self.logger.info("Database set to {}".format(db_name))
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            self.logger.error(e)
        else:
            self.mydb.commit()

    def set_table(self, table_name):
        """
        This Function set table as default table
        :param table_name: table name
        :return:
        """
        try:
            self.table_name = table_name
            self.logger.info("Table set to {}".format(table_name))
        except Exception as e:
            self.logger.error(e)

    def create_table(self, table_name, t_val=None, db_name=None):
        """
        :param db_name:
        :param table_name: a string
        :param t_val: A dictionary consisting of column names : data types and additional constraints

        t_val = {
        'id':'int primary key',
        'name':'varchar(20)'
        }
        :return: None
        """
        try:
            if db_name is None:
                db_name = self.db_name
            self.set_database(db_name)
            if t_val is None:
                t_val = {'id': 'int', 'name': 'varchar(20)'}
                self.logger.warning("Using default values. Check t_val. It should be dictionary")
            string_val = ""
            for k, v in t_val.items():
                string_val = string_val + k + " " + v + ","
            string_val = string_val[:-1]
            query1 = """create table if not exists {} ({})""".format(table_name, string_val)
            self.cursor.execute(query1)
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            self.logger.error(e)
        else:
            self.mydb.commit()
            self.logger.info("Created table {}".format(table_name))

    def drop_table(self, table_name=None, db_name=None):
        """
        Drop the given table in given database
        :param db_name:
        :param table_name:
        :return:
        """
        try:
            if table_name is None:
                table_name = self.table_name
            if db_name is None:
                db_name = self.db_name
            self.set_database(db_name)
            query = """drop table {}""".format(table_name)
            self.cursor.execute(query)
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            self.logger.error(e)
        else:
            self.mydb.commit()
            self.logger.info("""{} table is dropped""".format(table_name))

    def get_table_records(self, table_name=None, db_name=None):
        """
        Return all records of table
        :param db_name:
        :param table_name:
        :return:
        """
        try:
            if table_name is None:
                table_name = self.table_name
            if db_name is None:
                db_name = self.db_name
            self.set_database(db_name)
            self.cursor.execute("""select * from {}""".format(table_name))
            data = self.cursor.fetchall()
            self.logger.info("{} records are fetched and returned".format(table_name))
            return data
        except Exception as e:
            self.mydb.rollback()
            self.logger.error(e)

    def get_tables(self, db_name=None):
        """
        This function returns all the tables present in given database
        :param db_name:
        :return:
        """
        try:
            if db_name is None:
                db_name = self.db_name
            self.set_database(db_name)
            self.cursor.execute("SHOW tables")
            lst = self.cursor.fetchall()

            table_list = []
            for i in lst:
                table_list.append(i[0])
            self.logger.info("Tables are fetched from database {} and returned".format(db_name))
            return table_list
        except Exception as e:
            self.mydb.rollback()
            self.logger.error(e)

    def is_table_present(self, table_name=None, db_name=None):
        """
        Check if the given table is present or not
        :param db_name:
        :param table_name:
        :return:
        """
        try:
            if table_name is None:
                table_name = self.table_name
            if db_name is None:
                db_name = self.db_name
            self.set_database(db_name)

            if table_name in self.get_tables(db_name):
                return True
            return False

        except Exception as e:
            self.mydb.rollback()
            self.logger.error(e)

    def get_table_fields(self, table_name=None, db_name=None):
        """
        Return fields of table
        :param table_name:
        :param db_name:
        :return:
        """
        try:
            if table_name is None:
                table_name = self.table_name
            if db_name is None:
                db_name = self.db_name
            self.set_database(db_name)
            self.cursor.execute("""describe {}""".format(table_name))
            data = self.cursor.fetchall()
            self.logger.info("fields of {} are fetched and returned".format(table_name))
            return data
        except Exception as e:
            self.mydb.rollback()
            self.logger.error(e)

    def insert_record(self, record, db_name=None, table_name=None):
        """
        Insert one record in the given table and database
        :param record: A tuple (1,"name","last_name")
        :param db_name:
        :param table_name:
        :return:
        """
        try:
            if table_name is None:
                table_name = self.table_name
            if db_name is None:
                db_name = self.db_name
            self.set_database(db_name)
            query = """insert into {} values{}""".format(table_name, tuple(record))
            self.cursor.execute(query)
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            self.logger.error(e)
        else:
            self.mydb.commit()
            self.logger.info("""{} record added in table {}""".format(record, table_name))

    def insert_records(self, records, db_name=None, table_name=None):
        """
        This function insert records in given table
        :param table_name: name of table
        :param db_name:
        :param records: read values in list of tuples [(1, 'shashwat'), (2, "'bansal'"), (3, "'svsvds'"),
        (4, "'sdvdsve'"), (5, "'sdvsrgwrgew'")]
        :return: None
        """

        try:
            if table_name is None:
                table_name = self.table_name
            if db_name is None:
                db_name = self.db_name
            self.set_database(db_name)

            s_perc = '%s,' * len(records[0])
            s_perc = s_perc[:-1]

            fields = self.get_table_fields(table_name, db_name)
            fields_names = tuple([i[0] for i in fields])
            query = """insert into {}({}) values({})""".format(table_name, ",".join(fields_names), s_perc)
            self.cursor.executemany(query, records)
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            self.logger.error(e)
        else:
            self.mydb.commit()
            self.logger.info("given records are added in table {}".format(table_name))

    def delete_all_records(self, db_name=None, table_name=None):
        """
        This function delete all records in given table
        :param db_name:
        :param table_name:
        :return:
        """
        try:
            if table_name is None:
                table_name = self.table_name
            if db_name is None:
                db_name = self.db_name
            self.set_database(db_name)
            query = """truncate {}""".format(table_name)
            self.cursor.execute(query)
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            self.logger.error(e)
        else:
            self.mydb.commit()
            self.logger.info("all records are deleted from {}".format(table_name))

    def delete_records(self, where_statement, db_name=None, table_name=None):
        """
        This fuction delete records in table bases on where statement
        :param where_statement: should be string like "product_id = 1 and product_name = 'name'"
        :param db_name:
        :param table_name:
        :return:
        """
        try:
            if table_name is None:
                table_name = self.table_name
            if db_name is None:
                db_name = self.db_name
            self.set_database(db_name)

            query = """delete from {} where {}""".format(table_name, where_statement)
            self.cursor.execute(query)
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            self.logger.error(e)
        else:
            self.mydb.commit()
            self.logger.info("records are deleted where {} from {}".format(where_statement, table_name))

    def fetch_record_on_query(self, query, db_name=None, table_name=None):
        """

        :param query: MySql query
        :param db_name:
        :param table_name:
        :return:
        """
        try:
            if table_name is None:
                table_name = self.table_name
            if db_name is None:
                db_name = self.db_name
            self.set_database(db_name)
            self.cursor.execute(query)
            data = self.cursor.fetchall()
            self.logger.info("{} records are fetched and returned".format(table_name))
            return data
        except Exception as e:
            self.mydb.rollback()
            self.logger.error(e)

    def close_connection(self):
        try:
            self.mydb.close()
            self.logger.info("Connection closed")
            self.logger.close_db()
        except Exception as e:
            self.logger.error(e)
