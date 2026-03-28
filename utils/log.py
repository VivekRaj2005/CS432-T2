import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(levelname)s] %(asctime)s : %(message)s')

file_handler = logging.FileHandler('logs.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)