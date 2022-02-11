import logging


class LogDBHandler(logging.Handler):
    def __init__(self, sql_conn, sql_cursor, table_name):
        """
        This is sql based logging system
        :param sql_conn: mysql.connector.connect()
        :param sql_cursor: cursor of connection
        :param table_name: string
        """
        try:
            logging.Handler.__init__(self)
            self.sql_conn = sql_conn
            self.sql_cursor = sql_cursor
            self.db_tbl_log = table_name
        except Exception as e:
            print(e)

    def emit(self, record):
        try:
            self.format(record)  # to get proper date format

            # To get traceback details
            exc_text = None
            if record.exc_text is None:
                pass
            else:
                exc_text = record.exc_text[record.exc_text.find('line'):]
                exc_text = exc_text.replace(',', "")
                exc_text = exc_text.replace('\n', "  ")

            # All values to add
            values = (
                record.levelno,
                record.levelname,
                record.asctime,
                record.filename,
                record.name,
                record.funcName,
                record.lineno,
                str(record.msg),
                record.thread,
                record.threadName
            )

            # Formatting Sql query
            sql = """insert into {} values{}""".format(self.db_tbl_log, values)
            sql = sql.replace('(', '( default, ')
            sql = sql.replace(')', ', null)' if exc_text is None else ", '{}')".format(str(exc_text)))

            # Executing and committing
            self.sql_cursor.execute(sql)
            self.sql_conn.commit()

        except Exception as e:
            self.sql_conn.rollback()
            print(e)
