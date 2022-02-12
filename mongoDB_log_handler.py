import logging


class LogDBHandler(logging.Handler):
    def __init__(self, client, database, collection):

        try:
            logging.Handler.__init__(self)
            self.client = client
            self.database = database
            self.collection = collection
        except Exception as e:
            print(e)

    def emit(self, record):
        try:
            self.format(record)  # to get proper date format

            # To get traceback details
            exc_text = None
            # if record.exc_text is None:
            #    pass
            # else:
            #    exc_text = record.exc_text[record.exc_text.find('line'):]
            #    exc_text = exc_text.replace(',', "")
            #    exc_text = exc_text.replace('\n', "  ")

            # All values to add
            record_to_insert = {
                'levelno': record.levelno,
                'levelname': record.levelname,
                'asctime': record.asctime,
                'filename': record.filename,
                'name': record.name,
                'funcName': record.funcName,
                'lineno': record.lineno,
                'msg': str(record.msg),
                'thread': record.thread,
                'threadName': record.threadName,
                'exc_text': record.exc_text
            }

            # Executing and committing
            self.collection.insert_one(record_to_insert)

        except Exception as e:
            print(e)
