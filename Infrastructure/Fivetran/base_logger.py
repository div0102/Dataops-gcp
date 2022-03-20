import logging

logging.basicConfig(format="%(asctime)s : %(levelname)s : %(name)s : %(message)s")


def log_info(__name__):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    return logger
