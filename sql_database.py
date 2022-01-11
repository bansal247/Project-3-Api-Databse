import mysql.connector as connection


class SqlConnection:

    def __init__(self, host, user, passwd):
        self.t_attr = {}
        self.db_name = None
        self.mydb = None
        self.t_names = []
        self.host = host
        self.user = user
        self.passwd = passwd
        self.cursor = None
        try:
            self.mydb = connection.connect(host=self.host, user=self.user, passwd=self.passwd, use_pure=True)
        except Exception as e:
            print(e)
        self.cursor = self.mydb.cursor()

    def use_database(self, db_name):
        self.db_name = db_name
        try:
            self.cursor.execute("use {}".format(db_name))
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            print(str(e))
        else:
            self.mydb.commit()

    def create_database(self, db_name):
        self.db_name = db_name
        try:
            query = "create database if not exists {}".format(db_name)
            self.cursor.execute(query)
            self.cursor.execute("use {}".format(db_name))
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            print(str(e))
        else:
            self.mydb.commit()

    def create_table(self, t_name, t_val=None):
        """
        :param t_name: accept a string
        :param t_val: accept a dictionary
        t_val = {
        'id':'int primary key',
        'name':'varchar(20)'
        }
        :return: None
        """
        if t_val is None:
            t_val = {'id': 'int', 'name': 'varchar(20)'}
        self.t_attr[t_name] = t_val.keys()
        self.t_names.append(t_name)
        try:
            string_val = ""
            for k, v in t_val.items():
                string_val = string_val + k + " " + v + ","
            string_val = string_val[:-1]
            query1 = "create table if not exists {} ({})".format(t_name, string_val)
            self.cursor.execute(query1)
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            print(str(e))
        else:
            self.mydb.commit()

    def add_values(self, vals, t_name):
        """
        :param t_name: name of table
        :param vals: read values in list of tuples
        :return: None

        vals = [(2,'bansal'),(3,'svsvds'),(4,'sdvdsve'),(5,'sdvsrgwrgew')]
        """
        if vals is None:
            vals = []
        string_val = ""
        for attr in self.t_attr[t_name]:
            string_val = string_val + attr + ","
        string_val = string_val[:-1]

        s_perc = '"%s",' * len(self.t_attr[t_name])
        s_perc = s_perc[:-1]
        try:
            query = 'insert into {}({}) values({})'.format(t_name, string_val, s_perc)
            self.cursor.executemany(query, vals)
        except Exception as e:
            self.mydb.rollback()
            self.mydb.close()
            print(str(e))
        else:
            self.mydb.commit()

    def return_table(self, t_name):
        try:
            self.cursor.execute('select * from {}'.format(t_name))
            return self.cursor.fetchall()
        except Exception as e:
            self.mydb.rollback()
            print(str(e))

    def drop_table(self, t_name):
        try:
            self.t_names.remove(t_name)
            self.cursor.execute('drop table {}'.format(t_name))
        except Exception as e:
            self.mydb.rollback()
            print(str(e))
        else:
            self.mydb.commit()

    def close_connection(self):
        try:
            self.mydb.close()
        except Exception as e:
            print(str(e))
