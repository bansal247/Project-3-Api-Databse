from cassandra_logger import Logger


def try_it():
    client_id = 'QDOuJwdheYgCynNmJZewkzMy'
    client_secret = 'U2gADDgFC0PlOgt8vEzU1.3sl93j3FN,45R8wpLkg66i,qu9YZu-4UBhHEDluMKqXRxADFDwd9UOIaMUz1Qs4Gx,gznWcZqsytZNDlO5zTEXt2D3u-2X2JIB7hY1kwnq'
    path_to_secure_connect_logger_zip = 'secure-connect-logger.zip'
    class_name = "Main Class"

    logger = Logger(client_id, client_secret, path_to_secure_connect_logger_zip, class_name, table_name='logs1')
    logger.initialize()

    try:
        1 / 0
    except Exception as e:
        logger.error(e, exc_info=True)
        logger.warning("This is a warning")

    for i in logger.get_all_logs('logs1'):
        print(i)

    logger.close_session()


if __name__ == '__main__':
    try_it()
