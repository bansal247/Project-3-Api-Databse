import logging


class LogDBHandler(logging.Handler):

    def __init__(self, sql_conn, sql_cursor, db_tbl_log):
        logging.Handler.__init__(self)
        self.sql_conn = sql_conn
        self.sql_cursor = sql_cursor
        self.db_tbl_log = db_tbl_log

    def emit(self, record):
        self.format(record)
        exc_text = None
        if record.exc_text is None:
            pass
        else:
            exc_text = record.exc_text[record.exc_text.find('line') :]
            exc_text = exc_text.replace(',', "")
            exc_text = exc_text.replace('\n', "  ")
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
        sql = "insert into {} values{}".format(self.db_tbl_log, values)
        sql = sql.replace('(', '( default, ')
        sql = sql.replace(')', ', null)' if exc_text is None else ", '{}')".format(str(exc_text)))
        try:
            self.sql_cursor.execute(sql)
            self.sql_conn.commit()
        except Exception as e:
            print(e)
