from sql_logger import Logger


def try_it():
    logger = Logger(host="localhost", user="root", passwd="Shashu@247", class_name="Main Class")
    logger.initialize()
    try:
        1 / 0
    except Exception as e:
        logger.error(e, exc_info=True)


if __name__ == '__main__':
    try_it()
