from mongoDB_logger import  Logger


def try_it():
    logger = Logger(user_name='shashu247', password='shashu247',
                    url='mongodb+srv://shashu247:<password>@cluster0.5yvca.mongodb.net/myFirstDatabase?retryWrites'
                        '=true&w=majority', class_name='Main Class', console_output=True)
    logger.initialize()
    logger.warning("this is a warning")


if __name__ == '__main__':
    try_it()
