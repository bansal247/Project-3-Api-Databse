import logging


class LogDBHandler(logging.Handler):
    def __init__(self, cluster, session, table_name):
        """

        :param cluster:
        :param session:
        :param table_name:
        """
        try:
            logging.Handler.__init__(self)
            self.cluster = cluster
            self.session = session
            self.db_tbl_log = table_name
            try:
                self.next_id = max(session.execute("select id from {}".format(table_name)).all())[0]
            except:
                self.next_id = 1
        except Exception as e:
            print(e)

    def emit(self, record):
        try:
            self.format(record)  # to get proper date format
            self.next_id += 1
            # To get traceback details
            if record.exc_text is None:
                exc_text = "null"
            else:
                exc_text = record.exc_text[record.exc_text.find('line'):]
                exc_text = exc_text.replace(',', "")
                exc_text = exc_text.replace('\n', "  ")

            # All values to add
            values = (
                self.next_id,
                record.levelno,
                record.levelname,
                record.asctime,
                record.filename,
                record.name,
                record.funcName,
                record.lineno,
                str(record.msg),
                record.thread,
                record.threadName,
                exc_text
            )

            # Formatting Sql query
            sql = """insert into {} (id, log_level,level_name,created_at,file_name,class_name,function_name,line_no,
            log_msg,thread_no,thread_name,error_text) values{}""".format(self.db_tbl_log, values)
            sql = sql.replace("'null'", "null")

            # Executing and committing
            self.session.execute(sql)

        except Exception as e:
            print(e)
